from __future__ import annotations

import requests
from flask import Flask, render_template, request

app = Flask(__name__)

FASTAPI_URL = "http://localhost:8000/query"


@app.route("/", methods=["GET", "POST"])
def index():
    result = None

    if request.method == "POST":
        query = request.form.get("query", "")
        mode = request.form.get("mode", "agentic")

        response = requests.post(
            FASTAPI_URL,
            json={
                "query": query,
                "mode": mode,
            },
            timeout=120,
        )

        result = response.json()

    return render_template(
        "index.html",
        result=result,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)