
import os
import zipfile
import subprocess
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 允许所有源访问

HDFS_PATH = "/user/hadoop"  # HDFS 目标路径
status_prep = ""
status_anal_popularity = ""
status_anal_preference = ""
status_anal_duration = ""
status_anal_wordFrequency = ""
status_anal_genreInfo = ""

@app.route('/process/prep', methods=['POST'])
def process_prep():
    if 'file' not in request.files:
        return jsonify({'message': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'message': 'No selected file'}), 400

    if file:
        try:
            # print('Receive file:', file.filename)

            # # Save the uploaded file
            # file.save(file.filename)

            # folder_name = 'data/SongDataset'

            # # Extract the contents of the ZIP file
            # with zipfile.ZipFile(file.filename, 'r') as zip_ref:
            #     zip_ref.extractall(folder_name)  # Extract to a folder named after the ZIP file

            # print('Extract file to:', folder_name)

            # os.remove(file.filename)  # Remove the uploaded ZIP file after extraction

            hdfs_path = os.path.join(HDFS_PATH, 'gui')
            global status_prep

            # Transfer the extracted files to HDFS
            # status_prep = 'Transferring files to HDFS ...'
            # subprocess.run([
            #     "hdfs", "dfs", "-put",
            #     os.path.join(os.getcwd(), folder_name),
            #     hdfs_path
            # ])

            # status_prep = 'Preprocessing songs ...'
            # subprocess.run([
            #     "hadoop", "jar",
            #     "jars/PrepSongs.jar",
            #     os.path.join(hdfs_path, "SongDataset", "songs"),
            #     os.path.join(hdfs_path, "SongDataset")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "songs.txt"),
            #     os.path.join(os.getcwd(), "data", "songs.txt")
            # ])

            # status_prep = 'Preprocessing lyrics ...'
            # subprocess.run([
            #     "hadoop", "jar",
            #     "jars/PrepLyrics.jar",
            #     os.path.join(hdfs_path, "SongDataset", "lyrics"),
            #     os.path.join(hdfs_path, "SongDataset")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "lyrics.txt"),
            #     os.path.join(os.getcwd(), "data", "lyrics.txt")
            # ])

            # status_prep = 'Preprocessing genres ...'
            # subprocess.run([
            #     "hadoop", "jar",
            #     "jars/PrepGenres.jar",
            #     os.path.join(hdfs_path, "SongDataset", "genres"),
            #     os.path.join(hdfs_path, "SongDataset")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "genres.txt"),
            #     os.path.join(os.getcwd(), "data", "genres.txt")
            # ])

            # status_prep = 'Preprocessing users ...'
            # subprocess.run([
            #     "hadoop", "jar",
            #     "jars/PrepUsers.jar",
            #     os.path.join(hdfs_path, "SongDataset", "users"),
            #     os.path.join(hdfs_path, "SongDataset")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "users.txt"),
            #     os.path.join(os.getcwd(), "data", "users.txt")
            # ])

            # status_prep = 'Filtering data ...'
            # subprocess.run([
            #     "hadoop", "jar",
            #     "jars/Filter.jar",
            #     os.path.join(hdfs_path, "SongDataset"),
            #     os.path.join(hdfs_path, "SongDataset")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "filtered_songs.txt"),
            #     os.path.join(os.getcwd(), "data", "filtered_songs.txt")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "filtered_lyrics.txt"),
            #     os.path.join(os.getcwd(), "data", "filtered_lyrics.txt")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "filtered_genres.txt"),
            #     os.path.join(os.getcwd(), "data", "filtered_genres.txt")
            # ])
            # subprocess.run([
            #     "hdfs", "dfs", "-get",
            #     os.path.join(hdfs_path, "SongDataset", "filtered_users.txt"),
            #     os.path.join(os.getcwd(), "data", "filtered_users.txt")
            # ])

            # with zipfile.ZipFile(os.path.join('data', 'preprocess.zip'), 'w', zipfile.ZIP_DEFLATED) as zipf:
            #     for filename in os.listdir('data'):
            #         if filename.endswith('.txt'):
            #             zipf.write(os.path.join('data', filename), arcname=filename)

            status_prep = 'Successful!'
            return jsonify({'message': 'Successful!'}), 200
        
        except Exception as e:
            status_prep = f'Error: {str(e)}'
            print(e)
            return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/process/anal/popularity', methods=['POST'])
def process_anal_popularity():
    try:
        hdfs_path = os.path.join(HDFS_PATH, 'gui')
        global status_anal_popularity

        # status_anal_popularity = "Analyzing popularity ..."
        # subprocess.run([
        #     "hadoop", "jar",
        #     os.path.join("jars", "Popularity.jar"),
        #     os.path.join(hdfs_path, "SongDataset"),
        #     os.path.join(hdfs_path, "result")
        # ])

        # subprocess.run([
        #     "hdfs", "dfs", "-get",
        #     os.path.join(hdfs_path, "result", "task21.txt"),
        #     os.path.join(os.getcwd(), "data", "task21.txt")
        # ])

        # with zipfile.ZipFile(os.path.join('data', 'analysis_popularity.zip'), 'w', zipfile.ZIP_DEFLATED) as zipf:
        #     for filename in os.listdir('data'):
        #         if filename in ['task21.txt']:
        #             zipf.write(os.path.join('data', filename), arcname=filename)

        status_anal_popularity = "Successful!"
        return jsonify({'message': 'Successful!'}), 200
    except Exception as e:
        status_anal_popularity = f'Error: {str(e)}'
        print(e)
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/process/anal/preference', methods=['POST'])
def process_anal_preference():
    try:
        hdfs_path = os.path.join(HDFS_PATH, 'gui')
        global status_anal_preference

        status_anal_preference = "Analyzing preference ..."
        subprocess.run([
            "hadoop", "jar",
            os.path.join("jars", "Preference.jar"),
            os.path.join(hdfs_path, "SongDataset"),
            os.path.join(hdfs_path, "result")
        ])

        subprocess.run([
            "hdfs", "dfs", "-get",
            os.path.join(hdfs_path, "result", "task22.txt"),
            os.path.join(os.getcwd(), "data", "task22.txt")
        ])

        with zipfile.ZipFile(os.path.join('data', 'analysis_preference.zip'), 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in os.listdir('data'):
                if filename in ['task22.txt']:
                    zipf.write(os.path.join('data', filename), arcname=filename)

        status_anal_preference = "Successful!"
        return jsonify({'message': 'Successful!'}), 200
    except Exception as e:
        status_anal_popularity = f'Error: {str(e)}'
        print(e)
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/process/anal/duration', methods=['POST'])
def process_anal_duration():
    try:
        hdfs_path = os.path.join(HDFS_PATH, 'gui')
        global status_anal_duration

        # status_anal_duration = "Analyzing duration ..."
        # subprocess.run([
        #     "hadoop", "jar",
        #     os.path.join("jars", "Duration.jar"),
        #     os.path.join(hdfs_path, "SongDataset"),
        #     os.path.join(hdfs_path, "result")
        # ])

        # subprocess.run([
        #     "hdfs", "dfs", "-get",
        #     os.path.join(hdfs_path, "result", "task23.txt"),
        #     os.path.join(os.getcwd(), "data", "task23.txt")
        # ])

        status_anal_duration = "Generating plot ..."
        subprocess.run([
            "python3",
            os.path.join(os.getcwd(), "src", "task23.py")
        ])

        with zipfile.ZipFile(os.path.join('data', 'analysis_duration.zip'), 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in os.listdir('data'):
                if filename in ['task23.txt', 'task23.png']:
                    zipf.write(os.path.join('data', filename), arcname=filename)

        status_anal_duration = "Successful!"
        return jsonify({'message': 'Successful!'}), 200
    except Exception as e:
        status_anal_popularity = f'Error: {str(e)}'
        print(e)
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/process/anal/wordFrequency', methods=['POST'])
def process_anal_wordFrequency():
    try:
        hdfs_path = os.path.join(HDFS_PATH, 'gui')
        global status_anal_wordFrequency

        # status_anal_wordFrequency = "Analyzing word frequency ..."
        # subprocess.run([
        #     "hadoop", "jar",
        #     os.path.join("jars", "WordFrequency.jar"),
        #     os.path.join(hdfs_path, "SongDataset"),
        #     os.path.join(hdfs_path, "result")
        # ])

        # status_anal_wordFrequency = "Finding most frequent genre ..."
        # subprocess.run([
        #     "hadoop", "jar",
        #     os.path.join("jars", "GenreCount.jar"),
        #     os.path.join(hdfs_path, "SongDataset"),
        #     os.path.join(hdfs_path, "result")
        # ])

        # subprocess.run([
        #     "hdfs", "dfs", "-get",
        #     os.path.join(hdfs_path, "result", "task24"),
        #     os.path.join(os.getcwd(), "data", "task24")
        # ])

        # subprocess.run([
        #     "hdfs", "dfs", "-get",
        #     os.path.join(hdfs_path, "result", "genre_highest.txt"),
        #     os.path.join(os.getcwd(), "data", "genre_highest.txt")
        # ])

        status_anal_wordFrequency = "Generating plot ..."
        subprocess.run([
            "python3",
            os.path.join(os.getcwd(), "src", "task24.py")
        ])

        with zipfile.ZipFile(os.path.join('data', 'analysis_genreInfo.zip'), 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in os.listdir('data'):
                if filename in ['task24.png']:
                    zipf.write(os.path.join('data', filename), arcname=filename)
            for root, dirs, files in os.walk('data/task24'):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname('data/task24'))
                    zipf.write(file_path, arcname)

        status_anal_wordFrequency = "Successful!"
        return jsonify({'message': 'Successful!'}), 200
    except Exception as e:
        status_anal_popularity = f'Error: {str(e)}'
        print(e)
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/process/anal/genreInfo', methods=['POST'])
def process_anal_genreInfo():
    try:
        hdfs_path = os.path.join(HDFS_PATH, 'gui')
        global status_anal_genreInfo

        status_anal_genreInfo = "Analyzing genre information ..."
        subprocess.run([
            "hadoop", "jar",
            os.path.join("jars", "GenreInfo.jar"),
            os.path.join(hdfs_path, "SongDataset"),
            os.path.join(hdfs_path, "result")
        ])

        subprocess.run([
            "hdfs", "dfs", "-get",
            os.path.join(hdfs_path, "result", "task25.txt"),
            os.path.join(os.getcwd(), "data", "task25.txt")
        ])

        status_anal_genreInfo = "Generating plot ..."
        subprocess.run([
            "python3",
            os.path.join(os.getcwd(), "src", "task25.py")
        ])

        with zipfile.ZipFile(os.path.join('data', 'analysis_genreInfo.zip'), 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in os.listdir('data'):
                if filename in ['task25.txt', 'task25.png']:
                    zipf.write(os.path.join('data', filename), arcname=filename)

        status_anal_genreInfo = "Successful!"
        return jsonify({'message': 'Successful!'}), 200
    except Exception as e:
        status_anal_popularity = f'Error: {str(e)}'
        print(e)
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/status/prep', methods=['GET'])
def get_status_prep():
    global status_prep
    return jsonify({'status': status_prep})

@app.route('/status/anal/popularity', methods=['GET'])
def get_status_anal_popularity():
    global status_anal_popularity
    return jsonify({'status': status_anal_popularity})

@app.route('/status/anal/preference', methods=['GET'])
def get_status_anal_preference():
    global status_anal_preference
    return jsonify({'status': status_anal_preference})

@app.route('/status/anal/duration', methods=['GET'])
def get_status_anal_duration():
    global status_anal_duration
    return jsonify({'status': status_anal_duration})

@app.route('/status/anal/wordFrequency', methods=['GET'])
def get_status_anal_wordFrequency():
    global status_anal_wordFrequency
    return jsonify({'status': status_anal_wordFrequency})

@app.route('/status/anal/genreInfo', methods=['GET'])
def get_status_anal_genreInfo():
    global status_anal_genreInfo
    return jsonify({'status': status_anal_genreInfo})

@app.route('/result/anal/popularity', methods=['GET'])
def get_result_anal_popularity():
    try:
        result_file_path = os.path.join(os.getcwd(), 'data', 'task21.txt')
        
        if not os.path.exists(result_file_path):
            return jsonify({'message': 'Result file not found'}), 404
        
        with open(result_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            lines = [line.strip() for line in lines]

        return jsonify(data=lines)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/result/anal/preference', methods=['GET'])
def get_result_anal_preference():
    try:
        result_file_path = os.path.join(os.getcwd(), 'data', 'task22.txt')
        
        if not os.path.exists(result_file_path):
            return jsonify({'message': 'Result file not found'}), 404
        
        with open(result_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            lines = [line.strip() for line in lines]

        return jsonify(data=lines)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/result/anal/duration', methods=['GET'])
def get_result_anal_duration():
    try:
        result_image_path = os.path.join(os.getcwd(), 'data', 'task23.png')
        
        if not os.path.exists(result_image_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(result_image_path, mimetype='image/png')
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/result/anal/wordFrequency', methods=['GET'])
def get_result_anal_wordFrequency():
    try:
        result_image_path = os.path.join(os.getcwd(), 'data', 'task24.png')
        
        if not os.path.exists(result_image_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(result_image_path, mimetype='image/png')
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/result/anal/genreInfo', methods=['GET'])
def get_result_anal_genreInfo():
    try:
        result_image_path = os.path.join(os.getcwd(), 'data', 'task25.png')
        
        if not os.path.exists(result_image_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(result_image_path, mimetype='image/png')
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/download/prep', methods=['GET'])
def download_prep():
    try:
        download_file_path = os.path.join(os.getcwd(), 'data', 'preprocess.zip')
        
        if not os.path.exists(download_file_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(download_file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/download/anal/popularity', methods=['GET'])
def download_anal_popularity():
    try:
        download_file_path = os.path.join(os.getcwd(), 'data', 'analysis_popularity.zip')
        
        if not os.path.exists(download_file_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(download_file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/download/anal/preference', methods=['GET'])
def download_anal_preference():
    try:
        download_file_path = os.path.join(os.getcwd(), 'data', 'analysis_preference.zip')
        
        if not os.path.exists(download_file_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(download_file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/download/anal/duration', methods=['GET'])
def download_anal_duration():
    try:
        download_file_path = os.path.join(os.getcwd(), 'data', 'analysis_duration.zip')
        
        if not os.path.exists(download_file_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(download_file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/download/anal/wordFrequency', methods=['GET'])
def download_anal_wordFrequency():
    try:
        download_file_path = os.path.join(os.getcwd(), 'data', 'analysis_wordFrequency.zip')
        
        if not os.path.exists(download_file_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(download_file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/download/anal/genreInfo', methods=['GET'])
def download_anal_genreInfo():
    try:
        download_file_path = os.path.join(os.getcwd(), 'data', 'analysis_genreInfo.zip')
        
        if not os.path.exists(download_file_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(download_file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
