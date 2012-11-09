#!/usr/bin/python
#coding: utf8
import urllib2
import re
import time, datetime
import MySQLdb as mdb

dates = dict() #page id:post date

def getoldest(): #renew dates and return page id and post date
    header   = {'Referer':'http://cnbeta.com/', 'X-Requested-With':'XMLHttpRequest'}
    re_block = re.compile(r'<dt class="topic" ><a href="/articles/\d{6,7}\.htm[\s\S]+?\|')
    re_id    = re.compile(r'/articles/(\d{6,7}).htm') #group 1
    re_date  = re.compile(r'2\d{3}-[01]\d-[0-3]\d [012]\d:[0-5]\d:[0-5]\d') 
    mindate  = datetime.datetime.now() - datetime.timedelta(days = 1, minutes = -1)

    for i in range(32):
        print('Reading %s page...' % i) #debug
        if i == 0:
            req = urllib2.Request('http://cnbeta.com/') # Home page
        elif i <= 5:
            req = urllib2.Request('http://cnbeta.com/newread.php?page=%s' % i, None, header) # auto next page
        else:
            req = urllib2.Request('http://cnbeta.com/storydata.php?pageID=%s' % ( i - 4 ), None, header ) # hard next page
        html = urllib2.urlopen(req).read().decode('gb2312', 'ignore')
        
        mblocks = re_block.finditer(html)
        for mblock in mblocks:
            html = mblock.group(0)
            # find date:
            mdate = re_date.search(html)
            if mdate is None:
                print("Waring: Can't find DATE in block by RE")
                continue
            date = datetime.datetime.strptime(mdate.group(0), '%Y-%m-%d %H:%M:%S')
            # check date:
            if date < mindate:
                return id, dates[id]
            else:
                # store id:date
                id = int(re_id.search(html).group(1))
                dates[id] = date
                #print(id, date) #debug

def getnext(since): # return page id and post date
    if len(dates) < 16:
        dates.clear() # update id-date list
        getoldest()

    for id in range(since + 1, since + 64):
        if id in dates:
            return id, dates[id]

def decode(html): # decode HTML entity
    re_encode = re.compile(r"\&#\d{4,6};")
    results = re_encode.findall(html)
    for old in results:
        new = unichr(int(old[2:-1]))
        html = html.replace(old, new, 1)
    return html

def saveit(c, id): #SQL cursor, page id
    re_all     = re.compile(r'\<dl\>[\s\S]*?\</dl\>') #所有留言
    re_name    = re.compile(ur'\x09.*? 发表于 ') #发布者姓名
    re_time    = re.compile(ur'(?<=发表于 ).{19}(?=\</span\>\</dt\>)') #发布时间
    re_content = re.compile(ur're_detail"\>\n\x09   [\s\S]*?\</dd\>') #留言内容
    re_updown  = re.compile(r'"\>\d{1,5}\</span\>') #支持/反对数
    sql = "INSERT INTO cnbeta_cbhcomment (sid, ranking, name, time, content, up, down) VALUES (%s,%s,%s,%s,%s,%s,%s)" 
    
    print('Saving %s ...' % id)
    try:
        rsp = urllib2.urlopen('http://www.cnbeta.com/comment/g_content/%s.html' % id)
    except urllib2.HTTPError, e:
        print("Waring: HTTP-%s" % e.code)
        return
    html = rsp.read().decode('utf-8','ignore')
    comments = re_all.findall(html)
    ranking = 0
    for comment in comments: #依次处理每个热门评论
        ranking += 1
        name = re_name.findall(comment)
        time = re_time.findall(comment)
        content = re_content.findall(comment)
        updown = re_updown.findall(comment)
        if len(name) != 1 or len(time) != 1 or len(content) != 1 or len(updown) != 2:
            print('!!! error: name/time/content != 1 or updown != 2')
            print('           id=%s ranking=%s' % (id, ranking))
            print('%s %s %s %s ' % ( len(name),len(time),len(content),len(updown)))
            continue
        c_name = name[0][1:-4]
        c_time = time[0]
        c_content = content[0][16:-5]
        c_up = int(updown[0][2:-7])
        c_down = int(updown[1][2:-7])

        c_content = decode(c_content)
        c_name = decode(c_name)

        args = (id, ranking, c_name, c_time, c_content, c_up, c_down)
        c.execute(sql, args)


def main():
    conn = mdb.connect('localhost', 'username', 'password', 'savedb', charset = 'utf8')
    c = conn.cursor()
    id, date = getoldest()
    while True:
        stime = date + datetime.timedelta(days = 1, minutes = -1) - datetime.datetime.now()
        if stime > datetime.timedelta(minutes = 3):
            print('Sleep %ss for %s' % (stime.total_seconds(), id))
            time.sleep(stime.total_seconds())
        saveit(c, id)
        conn.commit()
        del dates[id]
        id, date = getnext(id)

if __name__ == '__main__':
    main()
