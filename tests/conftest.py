import pytest
import dotenv
import logging
from _pytest.monkeypatch import MonkeyPatch
from toolbox.utlis.logging import set_config

from tests.paths import OUTPATH


@pytest.fixture(scope="module", autouse=True)
def setup_main_path():
    dotenv.load_dotenv()
    mp = MonkeyPatch()
    mp.setenv("DATA_PATH", str(OUTPATH))
    
    def configure_logging(logging_module):
        logging_module.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    set_config(configure_logging)

    yield
    mp.undo()