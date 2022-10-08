import pickle

class AbstractJob:
    
    def __init__(self, sql_dict, seed_1 = 0, seed_2 = 0):
        self.sql_dict = sql_dict
        self.seed_1 = seed_1
        self.seed_2 = seed_2
        self.cummulated = {}
        self.create_cummulated()
        self.easy_cummulated = {}
        self.create_easy()
        self.sql = ""
        self.has_dup = self.duplicates()
        
        
    def duplicates(self):
        temp = []
        
        for i in self.cummulated["FROM"]:
            if i in temp:
                return True
            else:
                temp.append(i)
        return False
    
    def get_sql(self):
        words = [("SELECT", True, self.select_to_sql),
                 ("FROM",True, self.from_to_sql),
                 ("WHERE", False, self.where_to_sql),
                 ("WHERE_JOIN", False, self.where_join_to_sql),
                 ("GROUP BY", False, self.group_to_sql),
                 ("ORDER BY", False, self.order_to_sql)]
        sql = ""
        for w in words:
            if (w[0] not in self.cummulated.keys()) and w[1] == True:
                if w[0] == "SELECT":
                    sql += "SELECT * "
                    continue
                else:
                    print(f"Not able to construct SQL because of missing {w[0]}-Statement")
                    break
            elif w[0] not in self.cummulated.keys():
                continue
            sql += w[2]()
        self.sql = sql

    def unique(self, l):
        result = []
        for u in l:
            over_all_same = False
            for r in result:
                same = True
                for key in u.keys():
                    if key in r.keys() and r[key] == u[key]:
                        continue
                    else:
                        same = False
                        break
                if same:
                    over_all_same = True
                    break
            if not over_all_same:
                result.append(u)
        return result
    
    def unique_order(self):
        result = []
        cols = []
        for u in self.cummulated["ORDER BY"]:
            if u["FIELD"] == "GROUP BY":
                for g in self.cummulated["GROUP BY"]:
                    if g not in cols:
                        cols.append(g)
                        result.append({"FIELD": g, "ORDER": u["ORDER"]})
                continue                                      
            if u["FIELD"] not in cols:
                result.append(u)
                cols.append(u["FIELD"])
                
        self.cummulated["ORDER BY"] = result
        
    def create_easy(self):
        for key in self.cummulated:
            if str(key) != "WHERE_JOIN":
                self.easy_cummulated[key] = self.cummulated[key]
            else:
                joins = []
                for field in self.cummulated["WHERE_JOIN"]:
                    temp = {}
                    temp["FIELD"] = str(field["FIELD"])
                    temp["LEFT"] = str(field["LEFT"].table_name)
                    temp["RIGHT"] = str(field["RIGHT"].table_name)
                    joins.append(temp)
                self.easy_cummulated["WHERE_JOIN"] = joins    
        
    def create_cummulated(self):
        for i in self.sql_dict:
            if not type(i) == dict:
                continue
            key = list(i.keys())[0]
            if key in self.cummulated.keys():
                self.cummulated[key].append(i[key])
            else:
                self.cummulated[key] = [i[key]]
        if "SELECT" in self.cummulated.keys():
            self.cummulated["SELECT"] = self.unique(self.cummulated["SELECT"])            
        if "GROUP BY" in self.cummulated.keys():
            self.cummulated["GROUP BY"] = list(set(self.cummulated["GROUP BY"]))
        if "ORDER BY" in self.cummulated.keys():
            self.unique_order()
        
        
    def select_to_sql(self):
        s = "SELECT "
        
        temp_list = []
        if "GROUP BY" in self.cummulated.keys():
            for field in self.cummulated["GROUP BY"]:
                if self.seed_1 % 10 == 0 and s == "SELECT ":
                    s += "TOP "+str(self.seed_2*10) +" "
                    self.cummulated["TOP"] = self.seed_2*10
                    self.easy_cummulated["TOP"] = self.seed_2*10
                    
                s += field+", "
                temp_list.append({"FIELD": field})
        
        for field in self.cummulated["SELECT"]:
            if "OPERATOR" in field.keys():
                s += field["OPERATOR"]+"("+field["FIELD"]+"), "
            else:
                if self.seed_1 % 10 == 0 and s == "SELECT ":
                    s += "TOP "+str(self.seed_2*10) +" "
                    self.cummulated["TOP"] = self.seed_2*10
                    self.easy_cummulated["TOP"] = self.seed_2*10
                s += field["FIELD"]+", "
        self.easy_cummulated["SELECT"] = self.unique(self.easy_cummulated["SELECT"] + temp_list)
        
        s = s[:-2]
        return s
    
    def from_to_sql(self):
        s = " FROM "
        for field in self.cummulated["FROM"]:
            s += field+", "
        s = s[:-2]
        return s             
    
    def where_to_sql(self):
        s = " WHERE "
        for field in self.cummulated["WHERE"]:
            if field["OPERATOR"] == "BETWEEN":
                s += f"{field['FIELD']} BETWEEN {field['VALUE'][0]} AND {field['VALUE'][1]} AND "
            elif field["OPERATOR"] == "SUBQUERY":
                s += field["SQL"] +" AND "
            else:
                s += f'{field["FIELD"]} {field["OPERATOR"]} {field["VALUE"]} AND '
            
        s = s[:-4]
        return s                     

    def where_join_to_sql(self):
        if "WHERE" in self.cummulated.keys():
            s = " AND "
        else:
            s = " WHERE " 
        for field in self.cummulated["WHERE_JOIN"]:
            f = field["FIELD"].split("_")[1]
            left = field["LEFT"].suffix+f
            right = field["RIGHT"].suffix+f
            s += left +" = " + right +" AND "            
            
        s = s[:-4]
        return s                     
    
    def group_to_sql(self):
        s = " GROUP BY "
        for field in self.cummulated["GROUP BY"]:
            s += field+", "
        return s[:-2]
    
    def order_to_sql(self):
        s = " ORDER BY "
        for field in self.cummulated["ORDER BY"]:
            if field["FIELD"] == "GROUP BY":
                for field_2 in self.cummulated["GROUP BY"]:
                    s += field_2+" "
                    if field["ORDER"] == "DESC":
                        s += field["ORDER"]
                    s += ", "
                continue
                
            else:
                s += field["FIELD"] + " "
            if field["ORDER"] == "DESC":
                s += field["ORDER"]
            s += ", "
        return s[:-2]
        
    def create_sbt_project(self, path, job_id):
        self.get_sql()
        path_full = path + "/" + job_id
        with open(path_full+".txt", "w") as f:
            f.write(self.sql)
        with open(path_full+".pickle", "wb") as f:
            pickle.dump(dict(self.easy_cummulated),f)
        print(self.sql)
        