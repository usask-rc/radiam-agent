import os
import platform
from PyPDF2 import PdfFileReader
from netCDF4 import Dataset
from PIL import Image
from PIL.ExifTags import TAGS
import olefile
from docx import Document
import openpyxl
import cftime
import magic


def parse_pdf(crawled_file):
    pdf = PdfFileReader(crawled_file)
    info = pdf.getDocumentInfo()
    return info

def parse_cdf(crawled_file):
    rootgrp = Dataset(crawled_file, "r")
    cdf_dict = {}
    for x in rootgrp.ncattrs():
        cdf_dict[x] = getattr(rootgrp, x)
    return cdf_dict

def parse_exif(crawled_file):
    return Image.open(crawled_file)._getexif()

def parse_ole(crawled_file):
    ole = olefile.OleFileIO(crawled_file)
    meta = ole.get_metadata()
    ole_dict = {"Title": meta.title, "Author": meta.author, "Template": meta.template, "Keywords": meta.keywords}
    return ole_dict

def parse_word(crawled_file):
    with open(crawled_file, 'rb') as word_file:
        document = Document(word_file)
    word_dict = {"Title": document.core_properties.title, "Author": document.core_properties.author, "Revision": document.core_properties.revision, "Keywords": document.core_properties.keywords}
    return word_dict

def parse_excel(crawled_file):
    wb = openpyxl.load_workbook(crawled_file)
    excel_dict = {"Title": wb.properties.title, "Creator": wb.properties.creator, "Keywords": wb.properties.keywords}
    return excel_dict

def object_to_utf8(obj):
    # Walk an object and ensure that all of its elements are utf8 encoded and do not contain nulls
    # str, int, float are all OK to return untouched; tuples not handled
    if isinstance(obj, bytes):
        return obj.decode('utf-8','ignore').replace('\x00', '')
    elif isinstance(obj, list):
        retval = []
        for v in obj:
            ret.append(object_to_utf8(v))
        return retval
    elif isinstance(obj, dict):
        retval = {}
        for k,v in obj.items():
            retval[k] = object_to_utf8(v)
        return retval
    else:
        return obj

def route_metadata_parser(crawled_file):
    cdf_mimetypes = ['application/cdf', 'application/x-cdf', 'application/x-netcdf']
    exif_mimetypes = ['image/jpeg', 'image/pjpeg', 'image/jp2', 'image/png']
    ole_mimetypes = ['application/msword', 'application/vnd.ms-excel', 'application/vnd.ms-powerpoint']

    detected = magic.from_file(crawled_file, mime=True)
    if detected == 'application/pdf':
        parsed_metadata = parse_pdf(crawled_file)
    elif detected in cdf_mimetypes:
        parsed_metadta = parse_cdf(crawled_file)
    elif detected in exif_mimetypes:
        parsed_metadata = parse_exif(crawled_file)
    elif detected in ole_mimetypes:
        parsed_metadata = parse_ole(crawled_file)
    elif detected == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
        parsed_metadata = parse_word(crawled_file)
    elif detected == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        parsed_metadata = parse_excel(crawled_file)
    else:
        return {}
    return object_to_utf8(parsed_metadata)
