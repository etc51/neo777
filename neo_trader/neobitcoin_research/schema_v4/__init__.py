"""Schema-v4 raw-only research materialization pipeline.

This package deliberately has no dependency on the legacy review materializer.
Every public materializer consumes :class:`CanonicalTimeline`, whose only inputs
are the seven archived raw datasets.
"""

from .candidates import CandidateConfig, CandidateMaterializer
from .features import FeatureConfig, FeatureMaterializer
from .golden import validate_golden_synthetic
from .pipeline import MaterializationResult, RawOnlyPipeline
from .simulation import (
    ExecutionConfig,
    ExecutionSimulator,
    OutcomeMaterializer,
    StopAndExitSimulator,
)
from .timeline import CanonicalEvent, CanonicalTimeline, TimelineError
from .workflow import SchemaV4WorkflowResult, create_schema_v4_golden_bundle

__all__ = [
    "CandidateConfig",
    "CandidateMaterializer",
    "CanonicalEvent",
    "CanonicalTimeline",
    "ExecutionConfig",
    "ExecutionSimulator",
    "FeatureConfig",
    "FeatureMaterializer",
    "validate_golden_synthetic",
    "MaterializationResult",
    "OutcomeMaterializer",
    "RawOnlyPipeline",
    "StopAndExitSimulator",
    "TimelineError",
    "SchemaV4WorkflowResult",
    "create_schema_v4_golden_bundle",
]
