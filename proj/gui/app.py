from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import zipfile
import subprocess

app = Flask(__name__)
CORS(app)  # 允许所有源访问

HDFS_PATH = "/user/hadoop"  # HDFS 目标路径
current_status = ""

@app.route('/process', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return jsonify({'message': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'message': 'No selected file'}), 400

    if file:
        try:
            print('Receive file:', file.filename)

            # Save the uploaded file
            file.save(file.filename)

            folder_name = 'data/SongDataset'

            # Extract the contents of the ZIP file
            with zipfile.ZipFile(file.filename, 'r') as zip_ref:
                zip_ref.extractall(folder_name)  # Extract to a folder named after the ZIP file

            print('Extract file to:', folder_name)

            os.remove(file.filename)  # Remove the uploaded ZIP file after extraction

            hdfs_path = os.path.join(HDFS_PATH, 'gui')
            global current_status

            # Transfer the extracted files to HDFS
            current_status = 'Transferring files to HDFS ...'
            subprocess.run([
                "hdfs", "dfs", "-put",
                os.path.join(os.getcwd(), folder_name),
                hdfs_path
            ])

            current_status = 'Preprocessing songs ...'
            subprocess.run([
                "hadoop", "jar",
                "jars/PrepSongs.jar",
                os.path.join(hdfs_path, "SongDataset", "songs"),
                os.path.join(hdfs_path, "result")
            ])
            subprocess.run([
                "hdfs", "dfs", "-get",
                os.path.join(hdfs_path, "result", "songs.txt"),
                os.path.join(os.getcwd(), "data", "songs.txt")
            ])

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
