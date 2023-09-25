import os
from datetime import datetime
from io import BytesIO
from urllib.request import urlopen
from django.core.files.uploadedfile import InMemoryUploadedFile

from django.utils.text import slugify
from requests.models import Response


def upload_to(instance, filename):
    return upload_to_app_based_folder(instance, filename)


def upload_to_app_based_folder(instance: object, filename: object) -> object:
    filename_base, filename_ext = os.path.splitext(filename)
    return "upload/{app}/{instance}/{year}/{month}/{name}{extension}".format(
        app=slugify(instance._meta.app_label),
        instance=slugify(instance._meta.model_name),
        year=datetime.now().strftime("%Y"),
        month=datetime.now().strftime("%m"),
        name=slugify(datetime.now().strftime("%d%H%M%S%f")),
        extension=filename_ext.lower(),
    )


def upload_to_without_rename(instance, filename):
    filename_base, filename_ext = os.path.splitext(filename)
    return "upload/{app}/{instance}/{year}/{month}/{name}{extension}".format(
        app=slugify(instance._meta.app_label),
        instance=slugify(instance._meta.model_name),
        year=datetime.now().strftime("%Y"),
        month=datetime.now().strftime("%m"),
        name=filename_base,
        extension=filename_ext.lower(),
    )


def load_image_from_url(file, filename):
    f = BytesIO()
    if isinstance(file, str):
        content = urlopen(file).read()
    elif isinstance(file, Response):
        content = file.content
    else:
        raise Exception('unknown file type.')
    f.write(content)
    return InMemoryUploadedFile(f, None, filename, None, len(content), None, None)
