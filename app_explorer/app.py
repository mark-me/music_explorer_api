import os

from flask import Flask, render_template, send_from_directory


app = Flask(__name__)

if __name__ == "__main__":
    app.run(debug=True)


@app.route("/")
@app.route("/home")
def home():
    """Home page"""
    return render_template("home.html")


if __name__ == "__main__":
    app.run(debug=True, port=88)