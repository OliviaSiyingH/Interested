import sqlite3
from flask import Flask, request, jsonify
import json
import pandas as pd
import requests
from flask_restx import Api, Resource, fields,reqparse,marshal_with,abort
from datetime import datetime, timedelta
import numpy as np

app = Flask(__name__)
api = Api(app)

@api.errorhandler(CustomException)
def resource_not_found(e):
    return jsonify(error=str(e)), 404

# #define links stur
resource_fields_link = {}
resource_fields_link['self']=fields.Nested(api.model('self',{"href":fields.String}))
resource_fields_link['previous'] =fields.Nested(api.model('previous',{"href":fields.String}))
resource_fields_link['next'] = fields.Nested(api.model('next',{"href":fields.String}))
# #define network
resource_fields_net = {}
resource_fields_net['id'] = fields.Integer
resource_fields_net['name'] = fields.String
resource_fields_net['country'] = fields.Nested(api.model('name', {
    "name": fields.String,
    "code": fields.String,
    "timezone": fields.String
}))

# #define schedule
resource_fields_sch ={}
resource_fields_sch['time'] = fields.String
resource_fields_sch['days'] = fields.List(fields.String)
#define rating
#resource_fields_rating = api.model("resource_fields_rating",)

tv_model = api.model('tv-shows', {
    'tvmaze-id': fields.Integer,
    'id': fields.Integer,
    'last-update': fields.String,
    'name': fields.String,
    'type': fields.String,
    'language': fields.String,
    'genres':fields.List(fields.String),
    'status':fields.String,
    'runtime': fields.Integer,
    'premiered': fields.String,
    'officialSite': fields.String,
    'schedule':fields.Nested(api.model('time',resource_fields_sch)),
    'rating':fields.Wildcard(fields.String),
    'weight': fields.Integer,
    'network': fields.Nested(api.model('id',resource_fields_net)),
    'summary': fields.String,
    "_links" : fields.Nested(api.model('self',resource_fields_link))
})

tv_update = api.model('tv-shows_update', {
    'id': fields.Integer,
    'last-update': fields.Integer,
    "_links" : fields.String})

tv_import = api.model('tv-shows_import', {
    'id': fields.Integer,
    'last-update': fields.String,
    'tvmaze-id': fields.Integer,
    "_links" : fields.Raw
    })    

tv_list = api.model('tv-shows_import', {
    'page': fields.Integer,
    'page-size': fields.Integer,
    "tv-shows":fields.List(fields.String),
    "_links" : fields.Raw
    })    

parser = reqparse.RequestParser()
parser.add_argument("name", type=str, action="split")
@api.route('/tv-shows/import')
class tv_import(Resource):
    @api.response(201, 'Created',tv_import)
    @api.expect(parser)
    def post(self):
        conn = sqlite3.connect('z5253264.db')
        result_all = pd.read_sql('SELECT * FROM tv_shows',conn)
        url = request.url 
        
        #print([url,path])
        # Create the values
        reture_req = ['id','last-update','tvmaze-id']
        last_tv = result_all[reture_req].iloc[-1]   
        last_tv[reture_req[0]] += 1
        last_tv[reture_req[1]] = daytime
        last_tv[reture_req[2]] += 1
        last_tv["_links"] = {"self": {
          "href": str(url)
        }
        }
        result_all.append(last_tv)
        result_all.to_sql(name='tv_shows',con = conn,index = False,if_exists ='replace')
        conn.close()
        return json.loads(last_tv.to_json()), 201

parser = reqparse.RequestParser()
parser.add_argument('id', type=int)
@api.route('/tv-shows/<int:id>')
class tv_show(Resource):
    @api.response(200,'OK')
    @api.response(400,'	Bad request')
    @api.response(404,'tv-shows doesn\'t exist')
    def get(self, id):
        conn = sqlite3.connect('z5253264.db')
        result_all = pd.read_sql('SELECT * FROM tv_shows',conn)
        result_all = pd.read_sql('SELECT * FROM tv_shows',conn)
        # for i in range(len(result_all)):
        #     result_all['']
        url = request.url_root 
        path = '/tv-shows/'
       
        if id == 0:
            result_all['_links'] = str({
                    "self": {"href": str(request.url)},
                "next": {"href": str(url)+path+str(id+1)}})
        elif id == len(result_all)-1:
            result_all['_links'] = str({
                    "self": {"href": str(request.url)},
                "previous": {"href": str(url)+path+str(id-1)}})
        else:
            result_all['_links'] = str({
                    "self": {"href": str(request.url)},
                "previous": {"href": str(url)+path+str(id-1)},
                "next": {"href": str(url)+path+str(id+1)}})
        if id not in result_all['id']:
            api.abort(404, "tv-shows {} doesn't exist".format(id))
        #result_all.to_sql(name='tv_shows',con = conn,index = False,if_exists ='replace')
        
        ind = result_all[result_all['id']==id].index.values[0]
        result_tv = result_all[result_all['id']==id].to_dict(orient = 'index')[ind]
        conn.close()
        result_tv['network']=eval(result_tv['network'])
        result_tv['_links']=eval(result_tv['_links'])
        result_tv['rating']=eval(result_tv['rating'])
        result_tv['schedule']=eval(result_tv['schedule'])
        result_tv['genres']=eval(result_tv['genres'])

        return result_tv

    def delete(self, id):
        conn = sqlite3.connect('z5253264.db')
        result_all = pd.read_sql('SELECT * FROM tv_shows',conn)
        if id not in result_all.id:
            api.abort(404, "tv-shows {} doesn't exist".format(id))
        drop_item = result_all[result_all['id']==id].index
        result_all.drop(drop_item, inplace=True)
        result_all.to_sql(name='tv_shows',con = conn,index = False,if_exists ='replace')
        conn.close()
        return {"message": "The tv show with id {} is removed.".format(id),\
             "id":"{}".format(id)}, 200

    @api.expect(tv_model)
    def patch(self, id):
        conn = sqlite3.connect('z5253264.db')
        result_all = pd.read_sql('SELECT * FROM tv_shows',conn)
        if id not in result_all.index:
            api.abort(404, "The tv show {} doesn't exist".format(id))

        # get the payload and convert it to a JSON
        payload_tv = request.json
        # Update the values
        for key in payload_tv:
            
            if type(payload_tv[key]) is list:
                result_itm = str(payload_tv[key])
                result_all.loc[id, key] = result_itm
                #print(type(str(payload_tv[key])))
            elif type(payload_tv[key]) is dict:
                result_all.loc[id, key] = str(payload_tv[key])   
            else:
                result_all.loc[id, key] = payload_tv[key]

        url = request.url
        result_all.loc[id, ["last-update"]] = str(daytime)
        #result_all['_links'] = str({"self":{"href":url}})
        
        result_all.to_sql(name='tv_shows',con = conn,index = False,if_exists ='replace')
        conn.close()
        result_tv = dict()
        result_tv['id'] = int(result_all.loc[id,["id"]])
        result_tv['last-update'] = json.loads(result_all.loc[id,["last-update"]].to_json())
        result_tv['_links'] = {"self":{"href":url}}
        #print(result_tv)
        #tv = result_tv.to_json()
       # print(type(tv))
        return result_tv,200




parsers = reqparse.RequestParser()
parsers.add_argument("order_by", type=str, action="split")
parsers.add_argument("page", type=int)
parsers.add_argument("page_size", type=int)
parsers.add_argument("filter", type=str, action="split")

@api.route('/tv-shows')
class tv_order(Resource):
    @api.response(200,'OK')
    @api.expect(parsers)
    def get(self):
        args = parsers.parse_args()
        ord_by,colum = [],[]
        colum_as,comn_sql =[],[]
        ord_sql=[]
        for i in range(len(args['order_by'])):
            ordby = args['order_by'][i]
            #print(ordby)
            if '-' == ordby[0]:
                ord_by.append('DESC')
            else:
                ord_by.append('ASC')
            if '-' in ordby[1:]:
                colum.append(str(ordby[1:].split('-')[0]))
                ord_sql.append(str(ordby[1:].split('-')[0])+ ' '+ord_by[i])
                
            else:
                colum.append(ordby[1:] )
                ord_sql.append(ordby[1:] + ' '+ord_by[i])
        
        filters = list(args['filter'])
        filters_order_avg = set(filters + colum)
        sql = 'SELECT {} FROM tv_shows ORDER BY {} '.format(','.join(filters_order_avg),','.join(ord_sql))
        conn = sqlite3.connect('z5253264.db')
        result_all = pd.read_sql(sql,conn)
     
        page = args['page']
        page_size = args['page_size']
        total_page = len(result_all)//page_size + 1
        result_all['page-size'] = page_size
        chunks =[]
        for i in range(total_page):
            result_all['page'] = i+1
            chunks.append(result_all[i*page_size:(i+1)*page_size])
        if len(chunks)>= page -1 and page!=0:
            tv_shows_ord = pd.DataFrame(chunks[page-1])
        else:
            tv_shows_ord = pd.DataFrame(chunks[page])
        body = list(args['filter'])
        return_tv = tv_shows_ord.loc[:,body]
        url = request.url
        responce = dict()
        responce['page'] = page
        responce['page-size'] =page_size
        responce['tv-shows'] = return_tv.to_dict(orient='records')
        if page == 1:
            responce['_links'] = {
                    "self": {"href": str(url)},
                "next": {"href": str(url)}}
        elif page == total_page:
            responce['_links'] = {
                    "self": {"href": str(url)},
                "previous": {"href": str(url)}}
        else:
            responce['_links'] = {
                    "self": {"href": str(url)},
                "previous": {"href": str(url)},
                "next": {"href": str(url)}}
        conn.close()
        return responce,200
        


if __name__ == '__main__':
    daytime = datetime.now().strftime('%Y-%m-%d:%H:%M')
    json_file = "tv_shows.json" 
    df = pd.read_json(json_file)
    df['last-update'] = daytime
    df.reset_index(inplace = True)
    df.columns = ['id',"tvmaze-id",'url', 'name', 'type', 'language', 'genres', 'status', 'runtime',  \
       'premiered', 'officialSite', 'schedule', 'rating', 'weight', 'network',  \
       'webChannel', 'externals', 'image', 'summary', 'updated', '_links', 'dvdCountry','last-update']
    df.drop(['webChannel','image','dvdCountry','url','externals','updated','_links'],inplace=True, axis=1)
    for i in df.select_dtypes(object).columns:
        df[i] = df[i].astype(str)
    for i in df.select_dtypes(include=['int64']):
        df[i] = df[i].astype(int)
    #connect db
    cnx = sqlite3.connect('z5253264.db')
    df.to_sql(name='tv_shows',con = cnx,index = False,if_exists ='replace')
    cnx.close()


    app.run(debug=True)