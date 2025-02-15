from typing import Any, List, Optional, Union

from pandasai.helpers.pgcache import Cache
from pandasai.helpers.memory import Memory
from pandasai.helpers.skills_manager import SkillsManager
from pandasai.schemas.df_config import Config
from pandasai.vectorstores.vectorstore import VectorStore

from ..connectors import BaseConnector


class PipelineContext:
    """
    Pass Context to the pipeline which is accessible to each step via kwargs
    """

    def __init__(
        self,
        data_source: str,
        dfs: List[BaseConnector],
        enforce_grouping: Optional[bool] = False,
        config: Optional[Union[Config, dict]] = None,
        memory: Optional[Memory] = None,
        normalized_memory: Optional[Memory] = None,
        skills_manager: Optional[SkillsManager] = None,
        cache: Optional[Cache] = None,
        vectorstore: VectorStore = None,
        initial_values: dict = None,
    ) -> None:
        if isinstance(config, dict):
            config = Config(**config)

        self.dfs = dfs
        self.memory = memory or Memory()
        self.enforce_grouping = enforce_grouping
        self.normalized_memory = normalized_memory or Memory()
        self.skills_manager = skills_manager or SkillsManager()

        if config.enable_cache:
            self.cache = cache if cache is not None else Cache()
        else:
            self.cache = None

        self.data_source = data_source

        self.config = config

        self.intermediate_values = initial_values or {}

        self.vectorstore = vectorstore

        self._initial_values = initial_values

    def reset_intermediate_values(self):
        self.intermediate_values = self._initial_values or {}

    def add(self, key: str, value: Any):
        self.intermediate_values[key] = value

    def add_many(self, values: dict):
        self.intermediate_values.update(values)

    def get(self, key: str, default: Any = ""):
        return self.intermediate_values.get(key, default)
