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

from django.forms import ModelForm
from sql.models import Instance
class InstanceForm(ModelForm):
    class Media:
        model = Instance
        js = (
            "jquery/jquery.min.js",
            "dist/js/utils.js",
        )
