import io
import zipfile
from typing import List, Dict, Optional


class MroZipClass:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def __get_compression_library(self, file_path: Optional[io.BytesIO] = None) -> type(zipfile):
        if file_path is None:
            file_path = io.BytesIO(open(self.file_path, 'rb').read())
        return zipfile if zipfile.is_zipfile(file_path) else None

    def scan_xml_list(self, file_path: Optional[io.BytesIO] = None, parent_path: Optional[List[str]] = None,
                      max_depth: Optional[int] = None) -> List[Dict[str, str]]:
        if file_path is None:
            file_path = io.BytesIO(open(self.file_path, 'rb').read())
        xml_list = []
        if max_depth is not None and parent_path and len(parent_path) >= max_depth:
            return xml_list
        zf = zipfile.ZipFile(file_path)
        for name in zf.namelist():
            if name.endswith('.xml'):
                path = parent_path if parent_path else []
                xml_list.append({'main': self.file_path, 'path': '->'.join(map(str, path)), 'xml_file': name})
            elif not name.endswith('/'):
                sub_path = parent_path + [name] if parent_path else [name]
                sub_file = io.BytesIO(zf.read(name))
                xml_list.extend(self.scan_xml_list(sub_file, sub_path, max_depth))
        return xml_list

    def read_xml_data(self, xml_info: Dict[str, str]) -> Optional[bytes]:
        if 'path' not in xml_info or 'xml_file' not in xml_info:
            return None
        path_list = xml_info['path'].split('->') if xml_info['path'] else []
        main_path = xml_info.get('main', self.file_path)
        if main_path is None:
            return None
        file_obj = io.BytesIO(open(main_path, 'rb').read())
        for path in path_list:
            zf = zipfile.ZipFile(file_obj)
            sub_file = io.BytesIO(zf.read(path))
            file_obj = sub_file
        with zipfile.ZipFile(file_obj, 'r') as zf:
            return zf.read(xml_info['xml_file'])
