import os

from flask import Flask, render_template, send_from_directory

from logger import logger

app = Flask(__name__)

if __name__ == "__main__":
    app.run(debug=True)


@app.route("/")
@app.route("/home")
def home():
    """Home page"""
    return render_template("home.html")

@app.route("/cdn/<path:filepath>")
def cdn(filepath):
    dir, filename = os.path.split(db_reader.decode(filepath))
    logger.info(f"Serve media CDN - Directory: {dir} - File: {filename}")
    return send_from_directory(dir, filename, as_attachment=False)


if __name__ == "__main__":
    app.run(debug=True, port=88)