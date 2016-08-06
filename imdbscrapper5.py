from flask import Flask

app=Flask(__name__)
@app.route('/')
def imdbdata():
    return("OK!")


if __name__ == "__main__":
    app.run()