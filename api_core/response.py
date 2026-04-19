from rest_framework import status
from rest_framework.response import Response


def success_response(data=None, detail="ok", status_code=status.HTTP_200_OK):
    if data is None:
        data = {}
    return Response({"detail": detail, "data": data}, status=status_code)
