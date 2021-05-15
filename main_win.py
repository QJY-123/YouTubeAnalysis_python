# -*-coding；utf-8 -*-
"""
作者:白开水
日期:2021年05月03日 10:54
"""
import configparser
import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import sys
import io
import json
import seaborn as sns
import numpy as np

# 解决matplotlib在Win系统显示中文的问题
plt.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'‐'显示为方块的问题
# 设置输出显示的行列数
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 10000)
pd.set_option('display.width', 10000)
# 改变标准输出的默认编码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ========================================================
# ==================   定义配置类     ======================
class ReadConfig:
    def __init__(self):
        file_path = 'dbConfig.ini'
        self.conf = configparser.ConfigParser()
        self.conf.read(file_path, encoding="utf8")

    # 获取配置区段
    def get_config_sections(self):
        return self.conf.sections()

    # 获取键列表
    def get_config_options(self, section):
        return self.conf.options(section)

    # 获取键值对列表
    def get_config_items(self, section):
        return self.conf.items(section)

    # 获取不同数据类型的值
    def get_config_str(self, section, option):
        return self.conf.get(section, option)

    def get_config_boolean(self, section, option):
        return self.conf.getboolean(section, option)

    def get_config_int(self, section, option):
        return self.conf.getint(section, option)

    def get_config_float(self, section, option):
        return self.conf.getfloat(section, option)
# =====================================================
# ====================  数据库操作  =====================
# 获取配置文件的信息连接数据库
rc = ReadConfig()
host = rc.get_config_str('DATABASE', 'host')
user = rc.get_config_str('DATABASE', 'username')
password = rc.get_config_str('DATABASE', 'password')
# 参数设置 DictCursor使输出为字典模式
config = dict(host=host, user=user, password=password,
              cursorclass=pymysql.cursors.DictCursor
              )
# 建立连接
conn = pymysql.Connect(**config)
# 自动确认commit True
conn.autocommit(1)
# 设置光标
cursor = conn.cursor()
# csv 格式输入 mysql 中
def csvTomysql(db_name, table_name, df):
    # 根据pandas自动识别type来设定table的type
    df = df[:][['video_id', 'category', 'channel_title', 'views']]
    # 创建database
    cursor.execute('CREATE DATABASE IF NOT EXISTS {}'.format(db_name))
    # 选择连接database
    conn.select_db(db_name)
    # 创建table
    cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))
    create_sqli = "create table {} (video_id varchar(45),category varchar(45)," \
                  "channel_title varchar(45), views int)".format(table_name)
    cursor.execute(create_sqli)
    # 提取数据转list
    values = df.values.tolist()
    # 根据columns个数
    s = ','.join(['%s' for _ in range(len(df.columns))])
    # executemany批量操作插入数据
    cursor.executemany('INSERT INTO {} VALUES ({})'.format(table_name, s), values)

# 从数据库获取数据并将其转化为dataframe
def get_df_from_db(sql):
        return pd.read_sql(sql, conn)

# ====================================================
# =============    数据预处理及数据清洗   ================
def data_Pretreatment(csvFile, jsonFile):
    # 读取csv文件
    video_df = pd.read_csv(csvFile)
    # 一、根据时间的格式将数据处理成时间类型，用于后续的操作
    video_df['trending_date'] = pd.to_datetime(video_df['trending_date'], format='%y.%d.%m')
    video_df["publish_time"] = pd.to_datetime(video_df["publish_time"], format="%Y-%m-%dT%H:%M:%S.%fZ")
    video_df["category_id"] = video_df["category_id"].astype(str)  # 转换成字符型

    # 二、数据清洗
    # 删除重复数据
    video_df = video_df.drop_duplicates()
    # 将列中为空或者null的个数统计出来，并将缺失值最多的排前
    total_csv = video_df.isnull().sum().sort_values(ascending=False)
    # 输出缺失值记录百分比：
    percent_csv = (video_df.isnull().sum() / video_df.isnull().count()).sort_values(ascending=False)
    missing_data1 = pd.concat([total_csv, percent_csv], axis=1, keys=['Total', 'Percent'])
    # print('各列缺失值的数量及其占比')
    # print(missing_data1.head(10))
    # 考虑到description所在列有缺失值，数据分析的时候将description去除。
    video_df = video_df[:][
        ['video_id', 'trending_date', 'title', 'channel_title', 'category_id', 'publish_time', 'tags',
         'views', 'likes', 'dislikes', 'comment_count']]

    # 三、获取目录名字典category_dict
    # 设置category名称列
    id_to_category = {}
    with open(jsonFile) as f:
        js = json.load(f)
        for category in js["items"]:
            id_to_category[category["id"]] = category["snippet"]["title"]
    video_df["category"] = video_df["category_id"].map(id_to_category)

    # category_id = pd.read_json(jsonFile)[:]["items"]
    # category_dict = {}
    # for item in category_id:
    #     snippet = item['snippet']
    #     id = item["id"]
    #     title = snippet["title"]
    #     category_dict[id] = title
    # video_df['category'] = video_df['category_id'].map(category_dict)

    # 三、利用已知的指标创建新变量
    # # 点赞量和不点赞量比例
    # video_df["like_dislike_ratio"] = video_df['likes'] / video_df['dislikes']
    # # 评论占比
    # video_df["perc_comment"] = video_df["comment_count"] / video_df["views"]
    # # 点击率
    # video_df['perc_click'] = (video_df['likes'] + video_df['dislikes']) / video_df['views']

    # 再次显示有缺失值的记录数
    total = video_df.isnull().sum().sort_values(ascending=False)
    # print('各列缺失值的数量')
    # print(total)
    # 只保留没有空值的行
    video_df = video_df.dropna(axis=0)
    # print(video_df[:][['category']])
    return video_df

# =======================================================
# ===========    任务一:按照指定列进行排名取top10   ===========
# 通过category获取top10.以视频数量作为排名依据
def get_top10_by_category(video_df,imageName):
    plt.figure('根据category排名top10')
    # 因为是根据发布视频的数量来对category进行排名，所以需要进行数据清洗，多条重复的video_id只保留一条
    print(video_df.duplicated(['video_id'])) # 判断video_id列是否有重复行，重复的显示为TRUE，
    video_df=video_df.drop_duplicates(['video_id']) # 去掉重复行
    print(video_df.duplicated(['video_id']))
    by_category = video_df.groupby(["category"]).size().sort_values(ascending=False).head(10)
    sns.barplot(by_category.values, by_category.index.values, palette="hls")
    plt.title("Top 10 category of YouTube")
    plt.xlabel("video count")
    plt.savefig('./output/images/' + imageName + '.png', dpi=2000)
    plt.show()
    return by_category

# 通过channel_title获取top10.以视频数量作为排名依据
def get_top10_by_channel_title(video_df,imageName):
    plt.figure('根据channel_title排名top10')
    # 因为是根据发布视频的数量来对channel_title进行排名，所以需要进行数据清洗，多条重复的video_id只保留一条
    video_df=video_df.drop_duplicates(['video_id'])    # 去掉重复行
    by_channel = video_df.groupby(["channel_title"]).size().sort_values(ascending=False).head(10)
    sns.barplot(by_channel.values, by_channel.index.values, palette='hls')
    plt.title("Top 10 channel_title of YouTube")
    plt.xlabel("video count")
    plt.savefig('./output/images/' + imageName + '.png', dpi=2000)
    plt.show()
    return by_channel

# 在四个国家之中根据category获取top10
def get_top10_by_category_inAllCountries(video_df1, video_df2, video_df3, video_df4,imageName):
    plt.figure('在四个国家的全部数据中根据category排名top10')
    video_df = pd.concat([video_df1, video_df2, video_df3, video_df4], axis=0)
    video_df=video_df.drop_duplicates(['video_id'])  # 去掉重复行
    by_category = video_df.groupby(["category"]).size().sort_values(ascending=False).head(10)
    sns.barplot(by_category.values, by_category.index.values, palette="hls")
    plt.title("Most Frenquent Trending Youtube Categories")
    plt.xlabel("video count")
    plt.savefig('./output/images/' + imageName + '.png', dpi=2000)
    plt.show()
    return by_category


# ========================================================
# ================    任务三：相关性分析    ==================
def relationship_of_theCols(video_df,imageName):
    # 提取likes, dislikes, comment_count, views的数据
    rs = pd.concat([pd.DataFrame(video_df.views), pd.DataFrame(video_df.likes), pd.DataFrame(video_df.dislikes),
                    pd.DataFrame(video_df.comment_count)], axis=1)
    print(rs.corr())
    # 可视化相关系数矩阵，理论：皮尔逊相关系数
    plt.figure('相关性分析')
    cm = np.corrcoef(rs.values.T)
    sns.set(font_scale=1.5)
    hm = sns.heatmap(cm,
                     cbar=True,
                     annot=True,
                     square=True,
                     fmt='.2f',
                     annot_kws={'size': 10},
                     yticklabels=rs.columns.values,
                     # 处理x轴的刻度标签，如果标签长度超过10个字符，用省略号代替
                     xticklabels=[label[:7] + '...' if len(label) > 10 else label for label in rs.columns.values],
                     cmap="Blues",
                     cbar_kws={"shrink": 1},
                     )
    plt.xticks(rotation=-90)  # 将字体进行旋转
    plt.yticks(rotation=360)  # 将字体进行旋转
    plt.savefig('./output/images/' + imageName + '.png', dpi=2000)
    plt.show()


# ========================================================
# =========    任务四：统计不同国家各月份分布视频数量    =========
def get_videosNum_monthly(video_df,imageName):
    # 获取月份
    def gm(s):
        return s.month
    # 添加月份列
    video_df['month'] = video_df['publish_time'].apply(lambda x: gm(x))
    # 根据月份进行分组统计每个月份发布的视频数量
    by_month = video_df.groupby(["month"]).size()
    plt.figure('统计不同国家各月份分布视频数量 ')
    sns.barplot(by_month.index, by_month.values, palette="hls")
    a = by_month.index
    b = by_month.values
    for a, b in zip(a, b):  # 设置数据标签并控制标签的位置。第一个参数设置标签的水平位置
        plt.text(a - 1, b, '%d' % b, ha='center', va='bottom', fontsize=10)
    plt.title('Number of videos released monthly')
    plt.ylabel('Number of videos')
    plt.savefig('./output/images/'+imageName+'.png', dpi=2000)
    plt.show()


# ========================================================
# ==========   任务二： 统计视频发布之后上榜的天数    ===========
def get_days_of_trending(video_df,imageName):
    # 首先获取video_id，publish_time，trending_date三列的所有属性
    data = video_df[:][['video_id', 'publish_time', 'trending_date']]
    # 根据video_id进行分组以及降序排序
    trending_days = data.groupby(['video_id']).size().sort_values(ascending=False)
    df = {'video_id': trending_days.index, 'treding_days': trending_days.values}
    df = pd.DataFrame(df)
    # 生成html文档
    get_htmlReport_of_assignment2(df,imageName)



# ========================================================
# ===================  自动生成HTML报告  ===================
class html_tmpl_Of_assignment2(object):
    """html报告"""
    HTML_TMPL = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>统计视频发布之后上榜的天数</title>
            <link href="http://libs.baidu.com/bootstrap/3.0.3/css/bootstrap.min.css" rel="stylesheet">
            <h1 style="font-family: Microsoft YaHei">统计结果</h1>
            <p class='attribute'><strong>统计视频数: </strong> %(video_nums)s</p>
            <style type="text/css" media="screen">
        body  { font-family: Microsoft YaHei,Tahoma,arial,helvetica,sans-serif;padding: 20px;}
        </style>
        </head>
        <body>
            <table id='result_table' class="table table-condensed table-bordered table-hover">
                <colgroup>
                    <col align='left' />
                    <col align='right' />
                </colgroup>
                <tr id='header_row' class="text-center success" style="font-weight: bold;font-size: 14px;">
                    <th>video_id</th>
                    <th>trendingDays</th>
                </tr>
                %(table_tr)s
            </table>
        </body>
        </html>"""

    TABLE_TMPL = """
        <tr>
            <td>%(video_id)s</td>
            <td>%(trendingDays)s</td>
        </tr>"""


def get_htmlReport_of_assignment2(video_df, imageName):
    html = html_tmpl_Of_assignment2()
    table_tr0 = ''
    count = 0
    video_ids = []
    daysValues = []
    for video_id in video_df['video_id'].values:
        video_ids.append(video_id)
    for value in video_df['treding_days'].values:
        daysValues.append(value)
    for i in range(len(video_ids)):
        table_td = html.TABLE_TMPL % dict(video_id=video_ids[i], trendingDays=daysValues[i])
        table_tr0 += table_td
        count += 1
    output = html.HTML_TMPL % dict(video_nums=count, table_tr=table_tr0)
    with open('output/html/' + imageName + '.html', 'wb') as f:
        f.write(output.encode('utf-8'))

# # 需要进行数据清洗。相同的video_id只保留最后上榜的一个记录。
# def get_top10_by_views(video_df):
#     plt.figure('根据views排名top10')
#     by_views = video_df[:][['video_id', 'views']].sort_values(by='views', ascending=False)
#     print(by_views)
#     # sns.barplot(by_views.values, by_views.index.values, palette='hls')
#     # plt.title("Top 10 category of YouTube")
#     # plt.xlabel("video count")
#     # plt.show()

if __name__ == '__main__':
    # 设置csv文件路径列表
    csvFiles = ['./data/GBvideos.csv', './data/CAvideos.csv', './data/USvideos.csv', './data/DEvideos.csv']
    # 设置json文件路径列表 (顺序与csv文件列表一致)
    jsonFiles = ['./data/GB_category_id.json', './data/CA_category_id.json', './data/US_category_id.json','./data/DE_category_id.json']
    # 获取四个国家的数据表，存放在一个列表video_dfs中
    video_dfs = []
    for i in range(4):
        video_dfs.append(data_Pretreatment(csvFiles[i], jsonFiles[i]))

    # # 数据库操作测试
    # cursor.execute('use python')
    # for i in range(4):
    #     csvTomysql('python','videos',video_dfs[i])
    # df = get_df_from_db('select * from videos')
    # print(df)

    # ===========  任务一  ==========
    # 根据categories获取top10
    get_top10_by_category(video_dfs[0],'assignment1_byCategory_GB')
    get_top10_by_category(video_dfs[1],'assignment1_byCategory_CA')
    get_top10_by_category(video_dfs[2],'assignment1_byCategory_US')
    get_top10_by_category(video_dfs[3],'assignment1_byCategory_DE')
    # 根据channel_title获取top10
    get_top10_by_channel_title(video_dfs[0],'assignment1_byChannelTitle_GB')
    get_top10_by_channel_title(video_dfs[1],'assignment1_byChannelTitle_CA')
    get_top10_by_channel_title(video_dfs[2],'assignment1_byChannelTitle_US')
    get_top10_by_channel_title(video_dfs[3],'assignment1_byChannelTitle_DE')

    # ===========  任务二  ==========
    # 获取视频上榜天数
    get_days_of_trending(video_dfs[0],'assignment2_GB')
    get_days_of_trending(video_dfs[1],'assignment2_CA')
    get_days_of_trending(video_dfs[2],'assignment2_US')
    get_days_of_trending(video_dfs[3],'assignment2_DE')

    # ===========  任务三  ==========
    # 相关性分析
    relationship_of_theCols(video_dfs[0],'assignment3_GB')
    relationship_of_theCols(video_dfs[1],'assignment3_CA')
    relationship_of_theCols(video_dfs[2],'assignment3_US')
    relationship_of_theCols(video_dfs[3],'assignment3_DE')

    # ===========  任务四  ==========
    # 按月份统计不同国家发布的视频数量
    get_videosNum_monthly(video_dfs[0],'assignment4_GB')
    get_videosNum_monthly(video_dfs[1],'assignment4_CA')
    get_videosNum_monthly(video_dfs[2],'assignment4_US')
    get_videosNum_monthly(video_dfs[3],'assignment4_DE')




