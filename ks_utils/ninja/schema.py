from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, Union

from pydantic import GetCoreSchemaHandler, BaseModel
from pydantic_core import core_schema
from pydantic_core.core_schema import ValidationInfo


@dataclass(frozen=True)
class ImageResizeValidator:

    class UrlResizeType(Enum):
        cdn = 'cdn'
        oss = 'oss'


    width: int = 750
    through: str = UrlResizeType.cdn

    def __get_pydantic_core_schema__(
        self, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.with_info_after_validator_function(
            self.validate, handler(source_type), field_name=handler.field_name
        )

    def __get_resize_url_prefix(self):
        if self.through == self.UrlResizeType.oss:
            return 'x-oss-process='
        return 'image_process='

    def validate(self, value, info: ValidationInfo):
        return f'{value}?{self.__get_resize_url_prefix()}resize,w_{self.width}/format,webp'

def image_resize_url(width: int):
    pic = Annotated[str, ImageResizeValidator(width=width)]
    return pic
