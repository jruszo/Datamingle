#!/usr/bin/python
# -*- coding:utf-8 -*-
"""
---------------------------------------------------------
@project: issacmarkArchery
@file: form
@date: 2021/12/30 17:43
@author: mayp
---------------------------------------------------------
"""

from django.forms import ModelForm, Textarea
from sql.models import Tunnel, Instance
from django.core.exceptions import ValidationError


class TunnelForm(ModelForm):
    class Meta:
        model = Tunnel
        fields = "__all__"
        widgets = {
            "PKey": Textarea(attrs={"cols": 40, "rows": 8}),
        }

    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data.get("pkey_path"):
            try:
                pkey_path = cleaned_data.get("pkey_path").read()
                if pkey_path:
                    cleaned_data["pkey"] = (
                        str(pkey_path, "utf-8").replace(r"\r", "").replace(r"\n", "")
                    )
            except IOError:
                raise ValidationError(
                    "The private key file does not exist. Select the option to clear "
                    "the private key path, then save again."
                )


class InstanceForm(ModelForm):
    class Media:
        model = Instance
        js = (
            "jquery/jquery.min.js",
            "dist/js/utils.js",
        )
