from flask import Flask, render_template
import meinheld
import ujson as json

app = Flask(__name__)

@app.route('/hello')
def index():
    return json.dumps({"hello": "world"})


meinheld.listen(("0.0.0.0", 8000))
meinheld.run(app)
