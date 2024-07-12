
import os
import zipfile
import subprocess
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 允许所有源访问

HDFS_PATH = "/user/hadoop"  # HDFS 目标路径
current_status = ""

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
            global current_status

            # Transfer the extracted files to HDFS
            # current_status = 'Transferring files to HDFS ...'
            # subprocess.run([
            #     "hdfs", "dfs", "-put",
            #     os.path.join(os.getcwd(), folder_name),
            #     hdfs_path
            # ])

            # current_status = 'Preprocessing songs ...'
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

            # current_status = 'Preprocessing lyrics ...'
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

            # current_status = 'Preprocessing genres ...'
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

            # current_status = 'Preprocessing users ...'
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

            # current_status = 'Filtering data ...'
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

            # with zipfile.ZipFile(os.path.join('data', 'songdata.zip'), 'w', zipfile.ZIP_DEFLATED) as zipf:
            #     for filename in os.listdir('data'):
            #         if filename.endswith('.txt'):
            #             zipf.write(os.path.join('data', filename), arcname=filename)

            current_status = 'Successful!'

            # subprocess.run([
            #     "hdfs", "dfs", "-rm", "-r",
            #     os.path.join("gui", "SongDataset")
            # ])

            return jsonify({'message': 'Successful!'}), 200
        
        except Exception as e:
            current_status = f'Error: {str(e)}'
            print(e)
            return jsonify({'message': f'Error: {str(e)}'}), 500

@app.route('/status', methods=['GET'])
def get_status():
    global current_status
    return jsonify({'status': current_status})

@app.route('/download/prep', methods=['GET'])
def download_prep():
    try:
        result_file_path = os.path.join(os.getcwd(), 'data', 'songdata.zip')
        
        if not os.path.exists(result_file_path):
            return jsonify({'message': 'Result file not found'}), 404

        return send_file(result_file_path, as_attachment=True)
    
    except Exception as e:
        return jsonify({'message': f'Error: {str(e)}'}), 500
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
