import logging

from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile
from xml.etree import ElementTree as ET
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from time_counter import time_counter


RowLevel = namedtuple('RowLevel', 'id, level')
RowObject = namedtuple('RowObject', 'id, name')


class XMLFile:
    def __init__(self, fs_path):
        self.path = Path(fs_path)

    def parse(self):
        tree = ET.parse(self.path)
        root_el = tree.getroot()

        return (
            self.parse_id(root_el),
            self.parse_level(root_el),
            self.parse_object_name_queue(root_el)
        )

    def parse_id(self, root_el):
        el = root_el.find('./var[@name="id"]')

        assert el is not None

        return el.get('value')

    def parse_level(self, root_el):
        el = root_el.find('./var[@name="level"]')

        assert el is not None

        return el.get('value')

    def parse_object_name_queue(self, root_el):
        return tuple(
            object_el.get('name')
            for object_el in root_el.findall('./objects/object')
        )


class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(
            f'{self.__module__}.{self.__class__.__name__}'
        )

    @time_counter
    def __call__(self, work_dir):
        result_level = []
        result_object = []

        work_dir = Path(work_dir)

        # На моём железе последовательная обработка вышла даже бысстрее:
        # Последовательно:
        # Duration data_processor.__call__: 0.9753596782684326s
        # Duration reporter.__call__: 0.09128046035766602s
        #
        # Паралллеьно:
        # Duration data_processor.__call__: 2.069291114807129s
        # Duration reporter.__call__: 0.08977437019348145s
        with ThreadPoolExecutor() as executor:
            feature_queue = []

            for zip_path in work_dir.glob('*.zip'):
                feature_queue.append(executor.submit(
                    self._process_zip_archive,
                    work_dir,
                    zip_path
                ))

            for future in as_completed(feature_queue):
                result_level_queue, result_object_queue = future.result()

                result_level.extend(result_level_queue)
                result_object.extend(result_object_queue)

        return result_level, result_object

    def _process_zip_archive(self, work_dir, zip_path):
        result_level = []
        result_object = []

        self.logger.info(f'Process zip: {zip_path}')

        with TemporaryDirectory(dir=work_dir) as temp_dir:
            tmp_dir_path = Path(temp_dir)
            with ZipFile(zip_path) as zip_fp:
                zip_fp.extractall(tmp_dir_path)

            for xml_path in tmp_dir_path.glob('*.xml'):
                row_level, row_object_queue = self._process_xml(xml_path)
                result_level.append(row_level)
                result_object.extend(row_object_queue)

        return result_level, result_object

    def _process_xml(self, xml_path):
        self.logger.info(f'Process xml: {xml_path}')

        xml_file = XMLFile(xml_path)

        xml_id, xml_level, xml_object_name_queue = xml_file.parse()

        return (
            RowLevel(id=xml_id, level=xml_level),
            list(
                RowObject(id=xml_id, name=xml_object_name)
                for xml_object_name in xml_object_name_queue
            )
        )
