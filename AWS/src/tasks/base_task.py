from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseTask(ABC):
    @abstractmethod
    def execute(self, args: Any, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute the specific task logic.
        :param args: The parsed command line arguments.
        :param context: A dictionary containing shared objects like 'proxies' and 'asins'.
        :return: A list of result dictionaries.
        """
        pass
