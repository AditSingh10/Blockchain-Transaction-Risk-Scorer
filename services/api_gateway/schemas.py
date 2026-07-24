from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class ControlMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ThresholdMessage(ControlMessage):
    type: Literal["set_threshold"]
    value: float = Field(ge=0.0, le=1.0)


class SpeedMessage(ControlMessage):
    type: Literal["set_speed"]
    interval: float = Field(gt=0.0, le=10.0)


class ReplayStatusMessage(ControlMessage):
    type: Literal["pause_replay", "resume_replay"]


CONTROL_MESSAGE_ADAPTER: TypeAdapter[ThresholdMessage | SpeedMessage | ReplayStatusMessage] = (
    TypeAdapter(ThresholdMessage | SpeedMessage | ReplayStatusMessage)
)
