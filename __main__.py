import logging

from pathlib import Path

from data_generator import DataGenerator
from data_processor import DataProcessor
from reporter import Reporter


REPO = Path(__file__).absolute().parent
DATA_DIR = REPO / 'data'


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s', level=logging.DEBUG
)
logger = logging.getLogger(__name__)


def main():
    logger.info('Generating date')
    DataGenerator()(
        work_dir=DATA_DIR,
        count_archives=50,
        count_xml_files=100
    )

    logger.info('Processing data')
    data_processor = DataProcessor()
    result_level, result_object = data_processor(DATA_DIR)

    if result_level and result_object:
        Reporter()(DATA_DIR, result_level, result_object)


if __name__ == '__main__':
    main()
