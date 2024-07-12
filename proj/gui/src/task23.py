import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns

def read_data(file_path):
    ranges = []
    counts = []
    with open(file_path, 'r') as file:
        for line in file:
            range_key, count = line.rsplit(',', 1)
            ranges.append(range_key.strip())
            counts.append(int(count))
    return ranges, counts

def plot_histogram(ranges, counts, output_file):
    # Use seaborn style
    sns.set(style="whitegrid")

    plt.figure(figsize=(12, 8))
    barplot = sns.barplot(x=ranges, y=counts, palette="viridis")

    # Add labels and title
    plt.xlabel('Duration', fontsize=14)
    plt.ylabel('Number of Song', fontsize=14)
    plt.title('Song Duration Distribution Plot', fontsize=18)
    plt.xticks(rotation=45, ha="right", fontsize=12)
    plt.yticks(fontsize=12)

    # Add counts above the bars
    for p in barplot.patches:
        barplot.annotate(format(p.get_height(), '.0f'),
                         (p.get_x() + p.get_width() / 2., p.get_height()),
                         ha = 'center', va = 'center',
                         xytext = (0, 9),
                         textcoords = 'offset points',
                         fontsize=12)

    plt.tight_layout()

    # Save the plot to a file
    plt.savefig(output_file)
    # plt.show()

def main():
    input_file = "data/task23.txt"
    output_file = "data/task23.png"
    ranges, counts = read_data(input_file)
    plot_histogram(ranges, counts, output_file)

if __name__ == "__main__":
    main()
