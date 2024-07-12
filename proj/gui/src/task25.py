import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

def read_data(file_path):
    genres = []
    energy = []
    tempo = []
    loudness = []
    duration = []
    danceability = []

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            genres.append(parts[0])
            energy.append(float(parts[1]))
            tempo.append(float(parts[2]))
            loudness.append(float(parts[3]))
            duration.append(float(parts[4]))
            danceability.append(float(parts[5]))

    return genres, energy, tempo, loudness, duration, danceability

def plot_line_chart(genres, energy, tempo, loudness, duration, danceability, output_file):
    sns.set(style="whitegrid")
    
    # Set default font to SimHei for Chinese characters
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    
    plt.figure(figsize=(14, 8))
    
    palette = sns.color_palette("bright", 5)
    
    plt.plot(genres, energy, marker='+', linestyle='-', color=palette[0], label='Energy')
    plt.plot(genres, tempo, marker='s', linestyle='-', color=palette[1], label='Tempo')
    plt.plot(genres, loudness, marker='^', linestyle='-', color=palette[2], label='Loudness')
    plt.plot(genres, duration, marker='o', linestyle='-', color=palette[3], label='Duration')
    plt.plot(genres, danceability, marker='x', linestyle='-', color=palette[4], label='Danceability')

    plt.title('Average Genre Information', fontsize=20, fontweight='bold')
    plt.xlabel('Genre', fontsize=16)
    plt.ylabel('Average Value', fontsize=16)
    plt.xticks(rotation=45, fontsize=12)
    plt.yticks(fontsize=12)
    plt.legend(loc='upper right', fontsize=12, frameon=True, shadow=True)

    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()
    plt.savefig(output_file)
    # plt.show()

def main():
    input_file = 'data/task25.txt'
    output_file = 'data/task25.png'
    genres, energy, tempo, loudness, duration, danceability = read_data(input_file)
    plot_line_chart(genres, energy, tempo, loudness, duration, danceability, output_file)

if __name__ == "__main__":
    main()
