from datetime import datetime
from io import BytesIO
from typing import Any

import pandas as pd
from django.db.models import QuerySet
from django.http import HttpResponse
from django.utils import timezone


def excel_download(qs: Any, fields: tuple, columns: list, sheet_name: str = 'exported', filename: str = 'exported.xlsx'):
    data = None
    if isinstance(qs, QuerySet):
        data = list(qs.values_list(*fields))
    if isinstance(qs, list):
        data = qs

    if not data:
        raise ValueError('qs should be QuerySet or list')

    for index, item in enumerate(data):
        data[index] = list(item)
        for k, v in enumerate(item):
            if isinstance(v, datetime):
                data[index][k] = timezone.make_naive(v).strftime("%Y-%m-%d %H:%M")

    df = pd.DataFrame(data, columns=columns)
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    writer.close()
    output.seek(0)

    response = HttpResponse(output, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response['Content-Type'] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    response["Content-Disposition"] = f"attachment; filename={filename}"

    return response
