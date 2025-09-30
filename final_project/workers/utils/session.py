# File: workers/utils/session.py

"""
Thread-aware session management for Slack conversations
Location: workers/utils/session.py
"""

import json
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import hashlib
import structlog
import redis.asyncio as redis

logger = structlog.get_logger()


class SessionManager:
    """
    Thread-aware session manager.

    Key features:
    - Each thread has its own context
    - New threads start fresh
    - DMs maintain continuous session
    - Channel messages without thread are ephemeral
    """

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.thread_session_ttl = 3600 * 8  # 8 hours for threads
        self.dm_session_ttl = 3600 * 4  # 4 hours for DMs
        self.ephemeral_ttl = 300  # 5 minutes for non-thread messages
        self.max_history_items = 5

    def _get_session_key(
            self,
            team_id: str,
            channel_id: str,
            user_id: str,
            thread_ts: Optional[str] = None
    ) -> Tuple[str, str]:
        """
        Generate session key based on context.

        Returns:
            (session_key, session_type)
        """
        # Direct messages - continuous session per user
        if channel_id.startswith('D'):  # DM channel
            return f"session:dm:{team_id}:{user_id}", "dm"

        # Thread - session per thread
        if thread_ts:
            return f"session:thread:{team_id}:{channel_id}:{thread_ts}", "thread"

        # Channel message without thread - ephemeral
        return f"session:ephemeral:{team_id}:{channel_id}:{user_id}", "ephemeral"

    def _get_ttl_for_type(self, session_type: str) -> int:
        """Get TTL based on session type"""
        ttl_map = {
            "dm": self.dm_session_ttl,
            "thread": self.thread_session_ttl,
            "ephemeral": self.ephemeral_ttl
        }
        return ttl_map.get(session_type, self.ephemeral_ttl)

    async def get_or_create_session(
            self,
            team_id: str,
            channel_id: str,
            user_id: str,
            thread_ts: Optional[str] = None,
            inherit_from_parent: bool = True
    ) -> Dict[str, Any]:
        """
        Get or create session with thread awareness.

        Args:
            inherit_from_parent: If True and this is a new thread,
                                inherit context from parent message
        """
        session_key, session_type = self._get_session_key(
            team_id, channel_id, user_id, thread_ts
        )

        # Try to get existing session
        session_data = await self.redis.get(session_key)

        if session_data:
            try:
                session = json.loads(session_data)
                logger.debug("session_found",
                             session_key=session_key,
                             type=session_type,
                             queries_count=len(session.get("queries", [])))
                return session
            except json.JSONDecodeError:
                logger.warning("corrupted_session", key=session_key)

        # Create new session
        session = {
            "session_id": session_key,
            "session_type": session_type,
            "team_id": team_id,
            "channel_id": channel_id,
            "user_id": user_id,
            "thread_ts": thread_ts,
            "created_at": datetime.utcnow().isoformat(),
            "queries": [],
            "context": {
                "last_dataset": None,
                "last_tables": [],
                "thread_topic": None  # Can store thread topic/theme
            }
        }

        # For new threads, optionally inherit from parent
        if inherit_from_parent and thread_ts and session_type == "thread":
            parent_session = await self._get_parent_session(
                team_id, channel_id, user_id
            )
            if parent_session and parent_session.get("context"):
                # Inherit only relevant context, not full history
                session["context"]["last_dataset"] = parent_session["context"].get("last_dataset")
                session["context"]["last_tables"] = parent_session["context"].get("last_tables", [])[:3]
                logger.debug("inherited_context_from_parent",
                             thread_ts=thread_ts,
                             dataset=session["context"]["last_dataset"])

        await self._save_session(session_key, session, session_type)
        logger.info("session_created",
                    session_id=session_key,
                    type=session_type,
                    thread_ts=thread_ts)

        return session

    async def _get_parent_session(
            self,
            team_id: str,
            channel_id: str,
            user_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get parent session (DM or ephemeral) for context inheritance"""
        # Try DM session first
        if channel_id.startswith('D'):
            dm_key = f"session:dm:{team_id}:{user_id}"
            dm_data = await self.redis.get(dm_key)
            if dm_data:
                try:
                    return json.loads(dm_data)
                except json.JSONDecodeError:
                    pass

        # Try ephemeral session
        ephemeral_key = f"session:ephemeral:{team_id}:{channel_id}:{user_id}"
        ephemeral_data = await self.redis.get(ephemeral_key)
        if ephemeral_data:
            try:
                return json.loads(ephemeral_data)
            except json.JSONDecodeError:
                pass

        return None

    async def add_query_result(
            self,
            team_id: str,
            channel_id: str,
            user_id: str,
            thread_ts: Optional[str],
            query: str,
            sql: str,
            tables_used: List[str],
            row_count: int,
            execution_time_ms: int
    ) -> None:
        """Add query to session history with thread awareness"""
        session = await self.get_or_create_session(
            team_id, channel_id, user_id, thread_ts
        )

        # Create minimal entry
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "query": query[:500],
            "query_fingerprint": self._get_query_fingerprint(query),
            "sql": sql[:1000],
            "tables": tables_used[:5],
            "row_count": row_count,
            "execution_ms": execution_time_ms
        }

        # Avoid duplicates
        queries = session.get("queries", [])
        if queries and queries[-1].get("query_fingerprint") == entry["query_fingerprint"]:
            logger.debug("duplicate_query_skipped",
                         fingerprint=entry["query_fingerprint"])
            return

        # Add to history
        queries.append(entry)

        # Keep only last N queries
        if len(queries) > self.max_history_items:
            queries = queries[-self.max_history_items:]

        session["queries"] = queries

        # Update context
        if tables_used:
            if "." in tables_used[0]:
                dataset = tables_used[0].split(".")[1]
                session["context"]["last_dataset"] = dataset

            recent_tables = list(dict.fromkeys(
                session["context"].get("last_tables", []) + tables_used
            ))[:10]
            session["context"]["last_tables"] = recent_tables

        # Update thread topic if it's the first query in thread
        if session["session_type"] == "thread" and len(queries) == 1:
            # Extract key terms from first query as thread topic
            session["context"]["thread_topic"] = self._extract_topic(query)

        # Save with appropriate TTL
        session_key = session["session_id"]
        session_type = session["session_type"]
        await self._save_session(session_key, session, session_type)

        logger.info("query_added_to_session",
                    user_id=user_id,
                    session_type=session_type,
                    thread_ts=thread_ts,
                    tables_count=len(tables_used),
                    row_count=row_count)

    def _extract_topic(self, query: str) -> str:
        """Extract topic from query for thread context"""
        # Simple extraction - take first 50 chars
        # Could be enhanced with NLP
        return query[:50].replace('\n', ' ').strip()

    async def get_context_for_query(
            self,
            team_id: str,
            channel_id: str,
            user_id: str,
            thread_ts: Optional[str],
            current_query: str
    ) -> Dict[str, Any]:
        """Get relevant context for current query with detailed logging"""
        session = await self.get_or_create_session(
            team_id, channel_id, user_id, thread_ts
        )

        # Detect reference
        has_ref, ref_type = self._detect_context_reference(current_query)

        context = {
            "session_type": session["session_type"],
            "thread_ts": thread_ts,
            "has_reference": has_ref,
            "reference_type": ref_type,
            "last_dataset": session["context"].get("last_dataset"),
            "recent_tables": session["context"].get("last_tables", [])[:5],
            "thread_topic": session["context"].get("thread_topic"),
            "previous_queries": [],
            "queries_in_session": len(session.get("queries", []))
        }

        # Add previous queries based on session type
        queries = session.get("queries", [])
        if queries:
            if session["session_type"] == "thread":
                for q in queries:
                    context["previous_queries"].append({
                        "query": q["query"],
                        "sql": q["sql"],
                        "tables": q["tables"],
                        "row_count": q["row_count"]
                    })
            else:
                for q in queries[-2:]:
                    context["previous_queries"].append({
                        "query": q["query"],
                        "sql": q["sql"],
                        "tables": q["tables"],
                        "row_count": q["row_count"]
                    })

        # ДОБАВЛЕНО: Подробное логирование контекста
        logger.info("📋 SESSION CONTEXT RETRIEVED",
                    user_id=user_id,
                    session_type=session["session_type"],
                    thread_ts=thread_ts,
                    has_reference=has_ref,
                    reference_type=ref_type,
                    last_dataset=context["last_dataset"],
                    recent_tables=context["recent_tables"],
                    queries_in_session=context["queries_in_session"],
                    previous_queries_count=len(context["previous_queries"]))

        # Логируем предыдущие запросы
        for i, pq in enumerate(context["previous_queries"], 1):
            logger.debug(f"  Previous Q{i}",
                         query=pq["query"][:100],
                         tables=pq["tables"],
                         rows=pq["row_count"])

        return context

    def _detect_context_reference(self, query: str) -> Tuple[bool, str]:
        """Detect if query references previous context"""
        query_lower = query.lower()

        # Strong indicators
        strong_patterns = {
            "from these": "direct_reference",
            "from this": "direct_reference",
            "these results": "result_reference",
            "this data": "data_reference",
            "the above": "above_reference",
            "previous query": "previous_reference",
            "last query": "last_reference"
        }

        for pattern, ref_type in strong_patterns.items():
            if pattern in query_lower:
                return True, ref_type

        # Weak indicators (only in short queries)
        if len(query_lower) < 50:
            weak_starters = ["filter these", "group these", "sort this", "show only"]
            for starter in weak_starters:
                if query_lower.startswith(starter):
                    return True, "weak_reference"

        return False, "no_reference"

    def _get_query_fingerprint(self, query: str) -> str:
        """Generate query fingerprint for deduplication"""
        normalized = query.lower().strip()
        return hashlib.md5(normalized.encode()).hexdigest()[:8]

    async def _save_session(
            self,
            key: str,
            session: Dict[str, Any],
            session_type: str
    ) -> None:
        """Save session with appropriate TTL"""
        session["updated_at"] = datetime.utcnow().isoformat()

        ttl = self._get_ttl_for_type(session_type)

        await self.redis.setex(
            key,
            ttl,
            json.dumps(session)
        )

        logger.debug("session_saved",
                     key=key,
                     type=session_type,
                     ttl=ttl,
                     queries_count=len(session.get("queries", [])))

    async def clear_thread_session(
            self,
            team_id: str,
            channel_id: str,
            thread_ts: str
    ) -> None:
        """Clear specific thread session"""
        session_key = f"session:thread:{team_id}:{channel_id}:{thread_ts}"
        await self.redis.delete(session_key)
        logger.info("thread_session_cleared",
                    thread_ts=thread_ts,
                    channel_id=channel_id)

    async def get_session_stats(
            self,
            team_id: str
    ) -> Dict[str, Any]:
        """Get session statistics for monitoring"""
        pattern = f"session:*:{team_id}:*"
        keys = await self.redis.keys(pattern)

        stats = {
            "total_sessions": len(keys),
            "threads": 0,
            "dms": 0,
            "ephemeral": 0
        }

        for key in keys:
            if ":thread:" in key:
                stats["threads"] += 1
            elif ":dm:" in key:
                stats["dms"] += 1
            elif ":ephemeral:" in key:
                stats["ephemeral"] += 1

        return stats