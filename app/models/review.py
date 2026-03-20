"""
Re-exports DataAnomalyQueue and ProductionProjection for modules that import
from app.models.review rather than directly from app.models.correlation.

ManualReviewQueue is retained as a backward-compatible alias.  It will be
removed once all call sites have been updated to DataAnomalyQueue.
"""

from app.models.correlation import DataAnomalyQueue, ProductionProjection  # noqa: F401

# Backward-compatible alias — to be removed after full migration.
ManualReviewQueue = DataAnomalyQueue
