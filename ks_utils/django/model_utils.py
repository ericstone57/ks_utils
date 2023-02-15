import os
from datetime import datetime

from django.utils.text import slugify


def upload_to_app_based_folder(instance, filename):
    filename_base, filename_ext = os.path.splitext(filename)
    return "upload/{app}/{instance}/{year}/{month}/{name}{extension}".format(
        app=slugify(instance._meta.app_label),
        instance=slugify(instance._meta.model_name),
        year=datetime.now().strftime("%Y"),
        month=datetime.now().strftime("%m"),
        name=slugify(datetime.now().strftime("%d%H%M%S%f")),
        extension=filename_ext.lower(),
    )