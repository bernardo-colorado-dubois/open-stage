import logging
import re
import pandas as pd
from abc import abstractmethod
from io import StringIO
from open_stage.core.base import DataPackage, Pipe, Node, SingleInputMixin, SingleOutputMixin

logger = logging.getLogger(__name__)


class BasePromptTransformer(SingleInputMixin, SingleOutputMixin, Node):

    _SYSTEM_MESSAGE = (
        "You are a data transformation assistant. You will receive data in CSV format "
        "and transform it according to the user's instructions.\n"
        "CRITICAL: You must ALWAYS return your response as valid CSV format:\n"
        "- First line must be the header with column names\n"
        "- Following lines contain the data rows\n"
        "- Use comma as delimiter\n"
        "- Properly escape values containing commas or quotes\n"
        "- NEVER include explanations, markdown code blocks, or any text outside the CSV\n"
        "- Do not wrap the CSV in any formatting like ```csv\n"
        "- Return ONLY the raw CSV data"
    )

    def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int) -> None:
        super().__init__()
        self.name = name
        self.model = model
        self.api_key = api_key
        self.prompt = prompt
        self.max_tokens = max_tokens
        self.received_df = None
        self.client = None

        if not model or not model.strip():
            raise ValueError(f"{self.__class__.__name__} '{self.name}': model cannot be empty")
        if not api_key or not api_key.strip():
            raise ValueError(f"{self.__class__.__name__} '{self.name}': api_key cannot be empty")
        if not prompt or not prompt.strip():
            raise ValueError(f"{self.__class__.__name__} '{self.name}': prompt cannot be empty")
        if max_tokens <= 0:
            raise ValueError(f"{self.__class__.__name__} '{self.name}': max_tokens must be positive, got {max_tokens}")

    @abstractmethod
    def _initialize_client(self) -> None:
        pass

    @abstractmethod
    def _call_api(self, system_message: str, user_message: str) -> dict:
        """
        Call the LLM API. Must return a dict with:
          - response_text : str
          - truncated     : bool  (True if the model hit the token limit)
          - input_tokens  : int
          - output_tokens : int
        """
        pass

    def sink(self, data_package: DataPackage) -> None:
        df = data_package.get_df()
        logger.debug("%s '%s' received data from pipe '%s': %d rows, %d columns",
                     self.__class__.__name__, self.name, data_package.get_pipe_name(),
                     len(df), len(df.columns))
        self.received_df = df
        self.pump()

    def pump(self) -> None:
        if self.received_df is None:
            logger.warning("%s '%s' has no data to process", self.__class__.__name__, self.name)
            return
        if len(self.outputs) == 0:
            logger.warning("%s '%s' has no output pipe configured", self.__class__.__name__, self.name)
            return

        df = self.received_df

        try:
            if self.client is None:
                self._initialize_client()

            data_csv = df.to_csv(index=False)
            logger.info("%s '%s' sending request — model: %s, input: %d chars",
                        self.__class__.__name__, self.name, self.model, len(data_csv))

            user_message = (
                f"Here is the input data in CSV format:\n\n{data_csv}\n\n"
                f"Task: {self.prompt}\n\n"
                "Remember: Return ONLY raw CSV format, no explanations, no markdown, no code blocks."
            )

            result = self._call_api(self._SYSTEM_MESSAGE, user_message)

            response_text = result['response_text']
            truncated = result.get('truncated', False)
            input_tokens = result.get('input_tokens', 0)
            output_tokens = result.get('output_tokens', 0)

            logger.info("%s '%s' received response — %d chars, tokens: %d in / %d out",
                        self.__class__.__name__, self.name, len(response_text), input_tokens, output_tokens)
            logger.debug("Response preview: %s", response_text[:300])

            if truncated:
                logger.warning("%s '%s' response was truncated by token limit", self.__class__.__name__, self.name)

            result_df = self._parse_csv_response(response_text, truncated)

            logger.info("%s '%s' completed: %d rows → %d rows, columns: %s",
                        self.__class__.__name__, self.name, len(df), len(result_df), list(result_df.columns))

            output_pipe = list(self.outputs.values())[0]
            output_pipe.flow(result_df)
            logger.debug("%s '%s' pumped data through pipe '%s'", self.__class__.__name__, self.name, output_pipe.get_name())

        except Exception as e:
            logger.error("%s '%s' failed: %s: %s", self.__class__.__name__, self.name, type(e).__name__, e)
            raise

        finally:
            self.received_df = None

    def _parse_csv_response(self, response_text: str, truncated: bool) -> pd.DataFrame:
        response_text = response_text.strip()

        if response_text.startswith('```'):
            csv_match = re.search(r'```(?:csv)?\s*(.*?)\s*```', response_text, re.DOTALL)
            if csv_match:
                response_text = csv_match.group(1).strip()
                logger.debug("Removed markdown code block wrapper from response")

        if truncated:
            last_newline = response_text.rfind('\n')
            if last_newline != -1:
                response_text = response_text[:last_newline]
                logger.debug("Repaired CSV by removing incomplete last line")

        try:
            result_df = pd.read_csv(StringIO(response_text))
            logger.debug("Parsed CSV response: %d records", len(result_df))
            return result_df
        except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
            logger.error("Failed to parse CSV response: %s", e)
            logger.debug("Response preview (first 500 chars): %s", response_text[:500])
            raise
