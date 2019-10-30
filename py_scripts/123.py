import json

path = '/home/jiggalag/correction-rick-2018-09-27-000173f9.v1.json'

with open(path, 'r') as f:
    res = f.read()
    content = json.loads(res)
    print('stop')
    problem_campaigns = dict()
    campaigns = content.get('corrections')
    for item in campaigns:
        tmp_dict = dict()
        comment = item.get('comment').split('=')
        if 'inherited' in comment:
            tmp_dict.update({item.get('id'): {'comment': 'inherited'}})
            problem_campaigns.update(tmp_dict)
            continue
        before = float(comment[1][:comment[1].find('%')].replace(' ', '').replace(',', '.'))
        after = float(comment[2].replace('%', '').replace(',', '.'))
        if after > before:
            tmp_dict.update({item.get('id'): {'before': before, 'after': after}})
            problem_campaigns.update(tmp_dict)
    c = 0
    t = dict()
    for item in problem_campaigns:
        if 'comment' in problem_campaigns.get(item).keys():
            c += 1
        else:
            t.update({item: problem_campaigns.get(item)})
    print('Founded {} potentially problem campaigns'.format(len(problem_campaigns)))
