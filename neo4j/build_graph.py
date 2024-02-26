import os
import json
from py2neo import Graph, Node


class MyGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path = os.path.join(cur_dir, 'results/')
        self.g = Graph('http://124.222.20.223:7474', auth=('neo4j', 'Zym20020125'))

    def read_nodes(self):
        # 文章节点
        blogs = []
        # 分类节点
        categories = ["开发与运维", "算法", "操作系统", "人工智能", "数据库", "安全", "大数据", "云计算", "网络",
                      "微服务", "loT", "存储"]
        # 方面节点
        aspects = set()

        # 技术节点
        techs = []

        tech_names_set = set()

        # 数据中心和分类的关系
        rels_center_category = []
        # 文章和分类的关系
        rels_blog_category = []
        # 文章和技术的关系
        rels_blog_tech = []
        # 分类和技术的关系
        rels_category_tech = []
        # 技术和方面的正面情感关系
        rels_tech_aspect_pos = []
        # 技术和方面的负面情感关系
        rels_tech_aspect_neg = []
        # 技术和方面的中立情感关系
        rels_tech_aspect_mid = []

        # 数据中心和分类的关系
        for item in categories:
            rels_center_category.append(['阿里云开发者社区', item])

        count = 0
        file_names = os.listdir(self.data_path)
        for file_name in file_names:

            # 读取json文件信息
            file_path = os.path.join(self.data_path, file_name)
            data_json = None
            if file_name.endswith('.json'):
                with open(file_path, 'r') as file:
                    data_json = json.load(file)
            count += 1
            print(count)

            # 文章节点信息
            blog_node = {'blog_title': data_json['title'], 'blog_link': data_json['link']}
            blogs.append(blog_node)

            # 文章和分类的关系
            for category in data_json['category']:
                rels_blog_category.append([blog_node['blog_title'], category])

            for tech_json in data_json['tech']:

                if tech_json['name'] not in tech_names_set:
                    # 技术节点信息
                    tech_node = {'tech_name': tech_json['name'], 'tech_advantage': tech_json['优点'],
                                 'tech_disadvantage': tech_json['缺点'], 'tech_application': tech_json['应用场景'],
                                 'tech_development': tech_json['发展历程']}

                    techs.append(tech_node)
                    tech_names_set.add(tech_json['name'])

                    # 文章和技术的关系
                    rels_blog_tech.append([blog_node['blog_title'], tech_node['tech_name']])

                    # 分类和技术的关系
                    for category in data_json['category']:
                        rels_category_tech.append([category, tech_node['tech_name']])

                    # 方面节点信息
                    for absa in tech_json['absa']:
                        aspect_node = absa['aspect']
                        aspects.add(aspect_node)
                        if absa['senti'] == '正面的':
                            rels_tech_aspect_pos.append([tech_node['tech_name'], aspect_node])
                        elif absa['senti'] == '负面的':
                            rels_tech_aspect_neg.append([tech_node['tech_name'], aspect_node])
                        elif absa['senti'] == '中立的':
                            rels_tech_aspect_mid.append([tech_node['tech_name'], aspect_node])

        return blogs, categories, techs, tech_names_set, aspects, rels_center_category, rels_blog_category, \
            rels_blog_tech, rels_category_tech, rels_tech_aspect_pos, rels_tech_aspect_mid, rels_tech_aspect_neg

    '''建立普通节点'''

    def create_node(self, label, nodes):
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print(count, len(nodes))
        return

    '''建立文章节点'''

    def create_blog_node(self, blogs):
        count = 0
        for blog_node in blogs:
            node = Node('文章', name=blog_node['blog_title'], link=blog_node['blog_link'])
            self.g.create(node)
            count += 1
            print(count, len(blogs))
        return

    '''建立技术节点'''

    def create_tech_node(self, techs):
        count = 0
        for tech_node in techs:
            node = Node('技术', name=tech_node['tech_name'], advantage=tech_node['tech_advantage'],
                        disadvantage=tech_node['tech_disadvantage'],
                        application=tech_node['tech_application'], development=tech_node['tech_development'])
            self.g.create(node)
            count += 1
            print(count, len(techs))
        return

    '''创建实体关联边'''

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        count = 0
        total = len(edges)
        for edge in edges:
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, total)
            except Exception as e:
                print(e)
        return

    def create_nodes(self):
        blogs, categories, techs, tech_names_set, aspects, rels_center_category, rels_blog_category, rels_blog_tech, \
            rels_category_tech, rels_tech_aspect_pos, rels_tech_aspect_mid, rels_tech_aspect_neg = self.read_nodes()
        self.create_node('数据中心', ['阿里云开发者社区'])
        self.create_node('分类', categories)
        self.create_tech_node(techs)
        self.create_blog_node(blogs)
        self.create_node('方面', aspects)

    def create_relationships(self):
        blogs, categories, techs, tech_names_set, aspects, rels_center_category, rels_blog_category, rels_blog_tech, \
            rels_category_tech, rels_tech_aspect_pos, rels_tech_aspect_mid, rels_tech_aspect_neg = self.read_nodes()
        self.create_relationship('数据中心', '分类', rels_center_category, '包含分类', '包含分类')
        self.create_relationship('分类', '文章', rels_blog_category, '文章分类', '文章分类')
        self.create_relationship('文章', '技术', rels_blog_tech, '提及技术', '提及技术')
        self.create_relationship('分类', '技术', rels_category_tech, '有关技术', '有关技术')
        self.create_relationship('技术', '方面', rels_tech_aspect_pos, '正面的', '正面的')
        self.create_relationship('技术', '方面', rels_tech_aspect_mid, '中立的', '中立的')
        self.create_relationship('技术', '方面', rels_tech_aspect_neg, '负面的', '负面的')

    def export_data(self):
        blogs, categories, techs, tech_names_set, aspects, rels_center_category, rels_blog_category, rels_blog_tech, \
            rels_category_tech, rels_tech_aspect_pos, rels_tech_aspect_mid, rels_tech_aspect_neg = self.read_nodes()
        f_categories = open('categories.txt', 'w+')
        f_techs = open('techs.txt', 'w+')
        f_aspects = open('aspects.txt', 'w+')

        f_categories.write('\n'.join(list(categories)))
        f_techs.write('\n'.join(list(tech_names_set)))
        f_aspects.write('\n'.join(list(aspects)))

        f_categories.close()
        f_techs.close()
        f_aspects.close()

        return


if __name__ == '__main__':
    graph = MyGraph()
    graph.export_data()
    print("step1:导入图谱节点中")
    graph.create_nodes()
    print("step2:导入图谱边中")
    graph.create_relationships()
