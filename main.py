from flask import Flask , jsonify, render_template, request , stream_with_context , Response
from json import loads  
from kafka import KafkaConsumer
import json
from datetime import datetime
import time

my_consumer = KafkaConsumer(  
    'testnum',  
    bootstrap_servers = ['localhost : 9092'],  
    auto_offset_reset = 'earliest',  
    enable_auto_commit = True,  
    group_id = 'my-group',  
    value_deserializer = lambda x : loads(x.decode('utf-8'))  
    )

app = Flask(__name__)

@app.route('/')
def index():
	return render_template('index.html')

@app.route('/chart-data')
def chart_data():

    def generate_random_data():
        for message in my_consumer:
            message = message.value
            json_data = json.dumps(
                {'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'value': message})
            yield f"data:{json_data}\n\n"
            time.sleep(1)

    response = Response(stream_with_context(generate_random_data()), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response

if __name__ == '__main__':
	app.run(port=8000, debug=True, threaded=True)
