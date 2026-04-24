import logging
import google.generativeai as genai
from open_stage.core.base_ai import BasePromptTransformer

logger = logging.getLogger(__name__)


class GeminiPromptTransformer(BasePromptTransformer):

    def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int = 16000) -> None:
        super().__init__(name, model, api_key, prompt, max_tokens)

    def _initialize_client(self) -> None:
        try:
            genai.configure(api_key=self.api_key)
            self.client = genai.GenerativeModel(
                model_name=self.model,
                system_instruction=self._SYSTEM_MESSAGE,
            )
            logger.info("GeminiPromptTransformer '%s' client initialized (model=%s)", self.name, self.model)
        except Exception as e:
            raise ValueError(f"GeminiPromptTransformer '{self.name}' failed to initialize client: {str(e)}")

    def _call_api(self, system_message: str, user_message: str) -> dict:
        generation_config = genai.types.GenerationConfig(
            max_output_tokens=self.max_tokens,
            temperature=0.0,
            top_p=0.95,
            top_k=40,
        )
        response = self.client.generate_content(
            contents=user_message,
            generation_config=generation_config,
        )

        try:
            usage = response.usage_metadata
            input_tokens = usage.prompt_token_count
            output_tokens = usage.candidates_token_count
        except AttributeError:
            input_tokens = 0
            output_tokens = 0

        return {
            'response_text': response.text,
            'truncated': False,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
        }
