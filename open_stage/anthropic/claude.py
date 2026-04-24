import logging
import anthropic
from open_stage.core.base_ai import BasePromptTransformer

logger = logging.getLogger(__name__)


class AnthropicPromptTransformer(BasePromptTransformer):

    def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int = 16000) -> None:
        super().__init__(name, model, api_key, prompt, max_tokens)

    def _initialize_client(self) -> None:
        try:
            self.client = anthropic.Anthropic(api_key=self.api_key)
            logger.info("AnthropicPromptTransformer '%s' client initialized (model=%s)", self.name, self.model)
        except Exception as e:
            raise ValueError(f"AnthropicPromptTransformer '{self.name}' failed to initialize client: {str(e)}")

    def _call_api(self, system_message: str, user_message: str) -> dict:
        message = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system_message,
            messages=[{"role": "user", "content": user_message}]
        )
        return {
            'response_text': message.content[0].text,
            'truncated': message.stop_reason == "max_tokens",
            'input_tokens': message.usage.input_tokens,
            'output_tokens': message.usage.output_tokens,
        }
