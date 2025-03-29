#from typing import override
from openai import AssistantEventHandler
from logger_tt import getLogger

log = getLogger(__name__)

class EventHandler(AssistantEventHandler):
    """Custom event handler for processing assistant events."""

    def __init__(self):
        super().__init__()
        self.results = []  # Initialize the results list

    #@override
    def on_text_created(self, text) -> None:
        """Handle the event when text is first created."""
        # Append the created text to the results list
        self.results.append(text)

    #@override
    def on_text_delta(self, delta, snapshot):
        """Handle the event when there is a text delta (partial text)."""
        # Print the delta value (partial text) to the console
        #log.info(delta.value)
        # Append the delta value to the results list
        self.results.append(delta.value)

    def on_tool_call_created(self, tool_call):
        """Handle the event when a tool call is created."""
        # Print the type of the tool call to the console
        #log.info(f"\nassistant tool > {tool_call.type}\n")

    def on_tool_call_delta(self, delta, snapshot):
        """Handle the event when there is a delta (update) in a tool call."""
        if delta.type == 'code_interpreter':
            # Check if there is an input in the code interpreter delta
            if delta.code_interpreter.input:
                # Print the input to the console
                #log.info(delta.code_interpreter.input)
                # Append the input to the results list
                self.results.append(delta.code_interpreter.input)
            # Check if there are outputs in the code interpreter delta
            if delta.code_interpreter.outputs:
                # Print a label for outputs to the console
                #log.info("\n\noutput >")
                # Iterate over each output and handle logs specifically
                for output in delta.code_interpreter.outputs or []:
                    if output.type == "logs":
                        # Print the logs to the console
                        #log.info(f"\n{output.logs}")
                        # Append the logs to the results list
                        self.results.append(output.logs)