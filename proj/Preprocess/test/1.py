import re

# 读取 dependency-tree.dot 文件
with open('dependency-tree.dot', 'r') as file:
    dot_content = file.read()

# 正则表达式匹配依赖项
pattern = re.compile(r'"[^"]+" -> "([^:]+):([^:]+):([^:]+):([^"]+)"')
matches = pattern.findall(dot_content)

dependencies = set()
for match in matches:
    group_id, artifact_id, packaging, version = match
    dependencies.add((group_id, artifact_id, version))

# 生成 dependencies XML 元素
dependencies_xml = "\n".join(
    f"""<dependency>
        <groupId>{group_id}</groupId>
        <artifactId>{artifact_id}</artifactId>
        <version>{version}</version>
    </dependency>""" for group_id, artifact_id, version in dependencies
)

# 读取 pom.xml
with open('pom.xml', 'r') as file:
    pom_content = file.read()

# 在 <dependencies> 元素中插入新的依赖项
if '<dependencies>' in pom_content:
    pom_content = pom_content.replace('<dependencies>', f'<dependencies>\n{dependencies_xml}')
else:
    pom_content = pom_content.replace('</project>', f'<dependencies>\n{dependencies_xml}\n</dependencies>\n</project>')

# 将修改后的内容写回 pom.xml
with open('pom.xml', 'w') as file:
    file.write(pom_content)

print("Dependencies added to pom.xml successfully.")

