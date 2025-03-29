import openai
from openai import OpenAI
import pandas as pd
import time
import re
from secret import secrets

client = OpenAI(api_key=secrets.open_ai_key, timeout=60.0)
 
assistant = client.beta.assistants.create(
  instructions="You are a professional health economist. \
    You will be presented a document in Swedish describing the reasons for a reimbursement decision for a medicinal product. You should use your tools to extract info.",
  model="gpt-4o",
  tools= [
    {
      "type": "function",
      "function": {
            "name": "extract_info",
            "description": "Extracts useful information from a text document",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {
                        "type":"string",
                        "description": "Title of the document",
                    },
                    "diarie": {
                        "type":"string",
                        "description": "The diarie number of the document which has the format nnnn/YYYY",
                    }
                }
            }
        }
    },
    {
      "type": "function",
      "function": {
        "name": "get_rain_probability",
        "description": "Get the probability of rain for a specific location",
        "parameters": {
          "type": "object",
          "properties": {
            "location": {
              "type": "string",
              "description": "The city and state, e.g., San Francisco, CA"
            }
          },
          "required": ["location"],
          "additionalProperties": False
        },
        "strict": True
      }
    }
  ]
)