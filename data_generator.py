import random
import string
import logging

from zipfile import ZipFile
from tempfile import NamedTemporaryFile
from pathlib import Path
from xml.etree import ElementTree as ET
from uuid import uuid4

from time_counter import time_counter


def generate_random_string(length=20):
    return ''.join(
        random.choices(string.ascii_letters + string.digits, k=length)
    )


class XMLFile:
    def __init__(self, fs_path):
        self.path = Path(fs_path)

    def __call__(self):
        root_el = self.make_root_el()

        tree = ET.ElementTree(element=root_el)

        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True)

        with self.path.open(mode='wb') as fp:
            tree.write(fp, encoding='utf-8')

    def make_root_el(self):
        root_el = ET.Element('root')

        self.make_var_el(root_el, name='id', value=str(uuid4()))

        self.make_var_el(
            root_el,
            name='level',
            value=str(random.randint(1, 100))
        )

        self.make_objects_el(root_el, size=random.randint(1, 10))

        return root_el

    def make_var_el(self, parent_el, name, value):
        return ET.SubElement(
            parent_el,
            'var',
            attrib={'name': name, 'value': value}
        )

    def make_objects_el(self, parent_el, size):
        el = ET.SubElement(parent_el, 'objects')

        for _ in range(size):
            self.make_object_el(el, generate_random_string())

        return el

    def make_object_el(self, parent_el, name):
        return ET.SubElement(
            parent_el,
            'object',
            attrib={'name': name}
        )


class DataGenerator:
    def __init__(self):
        self.logger = logging.getLogger(
            f'{self.__module__}.{self.__class__.__name__}'
        )

    @time_counter
    def __call__(self, work_dir, count_archives, count_xml_files):
        for zip_index in range(count_archives):
            path = work_dir / f'{zip_index:03}.zip'

            self.logger.debug(f'Zip archive: {path}')

            with ZipFile(path, mode='w') as zip_fp:
                for xml_index in range(count_xml_files):
                    with NamedTemporaryFile(mode='w') as fp:
                        self.logger.debug(f'XML path: {fp.name}')

                        XMLFile(fp.name)()

                        zip_fp.write(fp.name, arcname=f'{xml_index:03}.xml')

                        self.logger.debug(f'xml saved as: {xml_index:03}.xml')
