from flask import Flask
import analyze_data

app = Flask("Charlie")


@app.route("/beat/<int:beat_id>")
def get_beat_data(beat_id):
	return analyze_data.get_data(beat_id)

if __name__ == "__main__":
	app.run()