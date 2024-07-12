import matplotlib.pyplot as plt
from wordcloud import WordCloud

genre = ""

with open("data/genre_highest.txt", 'r', encoding = "utf-8") as file:
    for line in file:
        genre = line.strip()

# 定义文件路径
file_path = "data/task24/" + genre + ".txt"

font_path = "/home/hadoop/.local/lib/python3.10/site-packages/wordcloud/DroidSansMono.ttf"

# 创建一个字典来存储词频
word_freq = {}

# 读取文件并统计词频
with open(file_path, 'r', encoding = "utf-8") as file:
    for line in file:
        word, freq = line.strip().split(',')
        word_freq[word] = int(freq)

# 生成词云图
wordcloud = WordCloud(width=800, height=400, background_color='white', font_path=font_path).generate_from_frequencies(word_freq)

# 显示词云图
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.savefig('data/task24.png', format='png')
# plt.show()
