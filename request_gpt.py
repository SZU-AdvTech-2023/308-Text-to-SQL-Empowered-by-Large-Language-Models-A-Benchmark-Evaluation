import openai

openai.api_key = "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"
openai.api_base = "https://api.openai.com/v1"
res = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        # {"role": "system", "content": "You are a helpful assistant that knows about the context."},
        {"role": "system", "content": "You are a helpful assistant that knows about the context."},
        {"role": "user", "content": "hello"}
    ]
)

print(res)