import pickle

class AbstractJob:
    
    def __init__(self, sql_dict, subtype, reference):
        self.sql_dict = sql_dict
        
        self.type = subtype
        self.reference = reference
        self.info = self.create_info()
        
        self.seed_1 = seed_1
        self.seed_2 = seed_2
        self.cummulated = {}
        self.create_cummulated()
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
                 ("WHERE_JOIN", False, self.where_join_to_sql)]
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
           
                
        # return self.JOB_TEMPLATE.replace("//#job_name#//", self.job_name).replace("//#body#//", self.job_body)
        # save plan is also replaced, but I have the feeling that this is not necessary here
        
    def save_dict(self):
        pass
        #return self.SBT_TEMPLATE.replace("//#job_name#//", self.job_name)

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
            self.cummulated["ORDER BY"] = self.unique(self.cummulated["ORDER BY"])
            
    def create_info(self):
         
        
    def select_to_sql(self):
        s = "SELECT "
        if "GROUP BY" in self.cummulated.keys():
            for field in self.cummulated["GROUP BY"]:
                if self.seed_1 % 10 == 0 and s == "SELECT":
                    s += "TOP "+str(self.seed_2*10) +" "
                    self.cummulated["TOP"] = self.seed_2*10
                    
                s += field+", "
        
        for field in self.cummulated["SELECT"]:
            if "OPERATOR" in field.keys():
                s += field["OPERATOR"]+"("+field["FIELD"]+"), "
            else:
                if self.seed_1 % 10 == 0 and s == "SELECT ":
                    s += "TOP "+str(self.seed_2*10) +" "
                    self.cummulated["TOP"] = self.seed_2*10
                s += field["FIELD"]+", "
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
                s += f"{field['FIELD']} BETWEEN {field['VALUE'][0]} AND {field['VALUE'][0]} AND "
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

        
    def create_sbt_project(self, path, job_id):
        self.get_sql()
        path_full = path + "/" + job_id
        with open(path_full+".txt", "w") as f:
            f.write(self.sql)
        with open(path_full+".pickle", "wb") as f:
            pickle.dump(self.cummulated ,f)
        with open(path_full+"_info"+".pickle", "wb") as f:
            pickle.dump(self.cummulated ,f)
        print(self.sql)
        