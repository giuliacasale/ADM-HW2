import pandas as pd
import random
import numpy as np
import matplotlib.pyplot as plt

#before starting: exploring datasets 

'''function that describes the columns of our datas and counts how many null values there are'''

def describe_df(df_names):
    months = ['October','November']
    for i in range(len(df_names)):
        df = pd.read_csv(df_names[i])
        print(f'Info for {months[i]}')
        print(df.info(),'\n')
        print("The total number of null values in each column is:")
        print(df.isnull().sum(),'\n')
        
#useful readerS

'''functions that read only the columns needed to answer
the query inside this question'''
def loadAllDatasets(df_names, lst_column):
    dfs = []
    for i in range(len(df_names)):
        dfs.append(pd.read_csv(df_names[i], usecols = lst_column)) #append and select the correct columns 
    df = pd.concat(dfs, ignore_index=True)
    return df

'''Only for subquery 1.4 and 1.5'''
def loadAllDatasetsWithParser(df_names):
    dfs = []
    for i in range(len(df_names)):
        dfs.append(pd.read_csv(df_names[i], usecols= ['user_session','event_type','product_id','event_time'], 
                                parse_dates= ["event_time"],
                                date_parser=pd.to_datetime))
    df = pd.concat(dfs, ignore_index=True)
    return df

def loadOneDataset(month, lst_column):
    return pd.read_csv(month, usecols = lst_column)

##RQ1

#1.1

'''This function picks only the rows corresponding to one specific 
event type. For each of them counts the total number that it appears
inide each user_session. To find the average it then divides that 
total with the total number of unique user_sessions inside the specifc
sub-dataframe.
At the end it plots a. graph showing the average times those operations
were performed inside the user_session for each event_type'''

def avg_operations_performed(df):
    only_v = df[df.event_type == 'view']
    only_p = df[df.event_type == 'purchase']
    only_c = df[df.event_type == 'cart']
    only_r = df[df.event_type == 'remove_from_cart']

    x = sum(only_v.groupby(['user_session','event_type']).event_type.count())
    y = sum(only_p.groupby(['user_session','event_type']).event_type.count())
    z = sum(only_c.groupby(['user_session','event_type']).event_type.count())
    w = sum(only_r.groupby(['user_session','event_type']).event_type.count())

    number_ses1 = len(only_v.user_session.unique())
    number_ses2 = len(only_p.user_session.unique())
    number_ses3 = len(only_c.user_session.unique())
    number_ses4 = len(only_r.user_session.unique())

    yax = [round((x/number_ses1),4),round((y/number_ses2),4),round((z/number_ses3),4),round((w/number_ses3),4)]
    xax = ['view','purchase','cart','remove from cart']

    plt.scatter(xax,yax)
    plt.plot(xax,yax,'-o')
    plt.ylabel = ('event average')
    plt.xlabel = ('events type')
    plt.title = ('operation users repeat on average within a session')
    plt.grid(color = 'lightblue',linestyle='-.')
    return plt.show()


#1.2

''' This function takes only the rows where the event_type is 'view' or
'cart'. We then,for each user_session and for each product_id, counted 
the number of event_type and we created two different data frames for it.
We took the intersection of them and to compute the average we divided 
the sum of all the cart events by the sum of all the view events '''

def avg_views_before_cart(df):
    only_v_and_c = df[(df.event_type == "view") | (df.event_type == "cart")].groupby([df.user_session, df.product_id, df.event_type]).event_type.count()
    
    only_v_and_c.name = 'count_type'
    only_v_and_c = only_v_and_c.reset_index("user_session").reset_index("product_id").reset_index("event_type")
    
    #new dataframes
    view = only_v_and_c[only_v_and_c.event_type == "view"]
    cart = only_v_and_c[only_v_and_c.event_type == "cart"]
    
    #dataframe created from the intersection of the two above
    merge_eventype = pd.merge(view, cart, how='inner', on =['product_id', 'user_session']) 

    avg = round(sum(merge_eventype["count_type_x"])/sum(merge_eventype["count_type_y"]),3)
    
    return avg


#1.3

''' This function takes only the rows where the event_type is 'cart' or
'purchase'. We then,for each user_session and for each product_id, counted 
the number of event_type and we created two different data frames for it.
We took the intersection of them and, to compute the average, we divided 
the length of the datframe intersected (which corresponds to the number
of purchases made) by the length of the only_cart dataframe (which 
corresponds to the number of times a product was added to a cart'''

def avg_purchase_after_cart(df):    
    only_c_and_p = df[(df.event_type == "cart") | (df.event_type == "purchase")].groupby([df.user_session, df.product_id, df.event_type]).event_type.count()
    
    only_c_and_p.name = 'count_type'
    only_c_and_p = only_c_and_p.reset_index("user_session").reset_index("product_id").reset_index("event_type")
    
    #new dataframes
    purchase = only_c_and_p[only_c_and_p.event_type == "purchase"]
    cart = only_c_and_p[only_c_and_p.event_type == "cart"]
    
    #dataframe created from the intersection of the two above
    merge_eventype = pd.merge(purchase, cart, how='inner', on =['product_id', 'user_session']) 

    avg = round(len(merge_eventype)/len(cart),3)
    return avg


#1.4

''' This function takes only the rows where the event_type is 'cart' or
'remove_from_cart'. We then, for each user_session, product_id and event_type, 
displayed  the event_time of that particular operation, and we created two 
different data frames for it. (one for cart and one for remove_from_cart)
We took the intersection of them and we added one new column: the one obtained
by the difference of times when the events on the same line happened converted
into seconds. Finally, to compute the average we divided the sum of the elements
of this new column by the number of rows of the dataframed of the intersection'''

def avg_time_cart_before_removal(df):
    only_c_and_r = df[(df.event_type == "remove_from_cart") | (df.event_type == "cart")].groupby([df.user_session, df.product_id, df.event_type, df.event_time]).event_time.count()

    only_c_and_r.name = "count_type"
    only_c_and_r = only_c_and_r.reset_index("user_session").reset_index("product_id").reset_index("event_type").reset_index("event_time")
    
    #new dataframes
    remove = only_c_and_r[only_c_and_r.event_type == "remove_from_cart"]
    cart = only_c_and_r[only_c_and_r.event_type == "cart"]
    
    #we deleted duplicates because one user could have viewed more than one time the same product
    #in the same session, but we decided to count only the time of the of the FIRST time the item
    #was added to the cart and the. FIRST time it was removed
    remove = merge_eventype.drop_duplicates(subset=['user_session','product_id'])
    cart = merge_eventype.drop_duplicates(subset=['user_session','product_id'])
    
    #dataframe created from the intersection of the two above
    merge_eventype = pd.merge(cart, remove, how='inner', on =['product_id', 'user_session']) 
    del merge_eventype["count_type_x"], merge_eventype["count_type_y"] 
    
    #creation of new column
    merge_eventype["difference_times"] = merge_eventype["event_time_y"] - merge_eventype["event_time_x"]
    merge_eventype["difference_times"] = merge_eventype["difference_times"].apply(lambda x : int(x.total_seconds()))
    
    diplay(merge_eventype)
    
    avg = (sum(merge_eventype["difference_times"]) / len(merge_eventype)) / 60
    return (f"The average time that a product stand into the cart before being removed is: {round(avg)} mins")

#1.5a

''' The two functions below take only the rows where the event_type is 'view' 
or 'cart'(first case)/'purchase'(second case). We then, for each user_session, 
product_id and event_type, displayed  the event_time of that particular operation, 
and we created two different data frames for it.
We took the intersection of them and we added one new column: the one obtained
by the difference of times when the events on the same line happened converted
into seconds. Finally, to compute the average we divided the sum of the elements
of this new column by the number of rows of the dataframed of the intersection'''

def avg_time_between_view_and_cart(df):
    only_v_and_c = df[(df.event_type == "view") | (df.event_type == "cart")].groupby([df.user_session, df.product_id, df.event_type, df.event_time]).event_time.count()

    only_v_and_c.name = 'count_type'
    only_v_and_c = only_v_and_c.reset_index("user_session").reset_index("product_id").reset_index("event_type").reset_index("event_time")

    #new dataframes
    cart = only_v_and_c[only_v_and_c.event_type == "cart"]
    view = only_v_and_c[only_v_and_c.event_type == "view"]
    
    #we deleted duplicates because one user could have viewed more than one time the same product
    #in the same session, but we decided to count only the time of the of the FIRST time the item
    #was added to the cart and the FIRST time it was viewed
    view = view.drop_duplicates(subset=['product_id', 'user_session'])
    cart = cart.drop_duplicates(subset=['product_id', 'user_session'])
    
    #dataframe created from the intersection of the two above
    merge_eventype = pd.merge(view, cart, how='inner', on =['product_id', 'user_session']) 
    del merge_eventype["count_type_x"], merge_eventype["count_type_y"] 
    
    #creation of new column
    merge_eventype["difference_times"] = merge_eventype["event_time_y"] - merge_eventype["event_time_x"]
    merge_eventype["difference_times"] = merge_eventype["difference_times"].apply(lambda x : int(x.total_seconds()))

    display(merge_eventype)

    avg = (sum(merge_eventype["difference_times"]) / len(merge_eventype)) / 60
    return round(avg)

#1.5b

def avg_time_between_view_and_purchase(df):
    only_v_and_p = df[(df.event_type == "view") | (df.event_type == "purchase")].groupby([df.user_session, df.product_id, df.event_type, df.event_time]).event_time.count()

    only_v_and_p.name = "count_type"
    only_v_and_p = only_v_and_p.reset_index("user_session").reset_index("product_id").reset_index("event_type").reset_index("event_time")
    
    #new dataframes
    purchase = only_v_and_p[only_v_and_p.event_type == "purchase"]
    view = only_v_and_p[only_v_and_p.event_type == "view"]
    
    #we deleted duplicates because one user could have viewed more than one time the same product
    #in the same session, but we decided to count only the time of the of the FIRST time the item
    #was purchased and the FIRST time it was viewed
    view = view.drop_duplicates(subset=['product_id', 'user_session'])
    purchase = purchase.drop_duplicates(subset=['product_id', 'user_session'])
    
    #dataframe created from the intersection of the two above
    merge_eventype = pd.merge(view, purchase, how='inner', on =['product_id', 'user_session']) 
    del merge_eventype["count_type_x"], merge_eventype["count_type_y"] 
    
    #creation of new column
    merge_eventype["difference_times"] = merge_eventype["event_time_y"] - merge_eventype["event_time_x"]
    merge_eventype["difference_times"] = merge_eventype["difference_times"].apply(lambda x : int(x.total_seconds()))
    
    display(merge_eventype)
    
    avg = (sum(merge_eventype["difference_times"]) / len(merge_eventype)) / 60
    return round(avg)


##RQ2

'''This function below needs to groupby each category with their score'''
def restrict_dt(month, eventype, n):
    dt = month.copy() #copy the properly month dataset to do our operations 
    
    dt = dt[(dt.event_type == eventype) & (dt.category_code.notnull())] #select the correct event_type and throw the NaN values

    dt["category"] = dt.category_code.str.split('.') #split the category_code by . and select the first element, because it is our category
    dt["category"] = dt["category"].apply(lambda x: x[0]) #extract only the first element of category_code

    return dt.groupby([dt.category]).product_id.count().sort_values(ascending=False).head(n) #return our request

'''Plot the categories most sold'''
def plot_n_categories(dt):
    dt.plot.bar(figsize=(18,6))

    plt.grid(color = 'lightgray', linestyle='-.')
    
    plt.xlabel('Categories')
    plt.ylabel('Sold products')
    
    plt.title(f'Top {len(dt)} Categories')
    
    plt.show()

#1st subquery

'''We obtain with this function below the groupified subcategories with their score'''

def dt_subcategories(month):
    dt = month.copy() #copy the properly month dataset to do our operations 

    dt["subcategory"] = dt.category_code.str.split('.') #split string to dot symbol
    dt = dt[dt.category_code.notnull()] #remove NaN values

    dt["subcategory"] = dt["subcategory"].apply(lambda x: x[1] if len(x)>1 else "NaN") #extract only the second element of category_code
    dt = dt[dt.subcategory.notnull()] #remove NaN values by subcategory

    return dt

'''Plot the subcategories most viewed'''
def plot_n_subcategories(dt, n):
    #restrict the dataset
    dt[dt.event_type == "view"].groupby([dt.subcategory]).product_id.count().sort_values(ascending=False).head(n).plot.bar(figsize=(18,6))
    
    #plot the n subcategories
    plt.grid()
    
    plt.xlabel('Subcategories')
    plt.ylabel('Viewed products')
    
    plt.title(f'Top {n} Subcategories')

    plt.show()

#2nd subquery

'''For each category print 10 most sold product, in this case we selected the event_type="purchase
and then we made our for loop to print the products most sold. In few rows we copy & paste the same
solution found for our other solutions to questions'''

def most_sold_products_per_category(month, n):
    dt = month.copy() #copy the properly month dataset to do our operations 
    
    dt = dt[(dt.event_type == "purchase") & (dt.category_code.notnull())] #select the correct event_type="purchase" and throw the NaN values

    dt["category"] = dt.category_code.str.split('.') #split the category_code by . and select the first element, because it is our category
    dt["category"] = dt["category"].apply(lambda x: x[0]) #extract only the first element of category_code

    for state, frame in list(dt.groupby(["category"]))[:5]: #print 5 most sold products per category 
        print(f"The category {state} has:")
        print(frame.groupby("product_id").count().sort_values("category_code",ascending=False).category_code.head(n))

##RQ3

'''function that selects a random category_code for which we want to apply 
the function below (avg_price)'''

def choose_category(df): 
    #list of all possibile category_codes in that specific month
    l = df.category_code.unique()
    #we select a random category_code from all the possible ones
    category = str(random.choice(l))
    return category
    

'''For every category_code specified in input, returns a
histogram that shows the average price for each brand that
is selling some products that fall in that specific category.'''

def avg_price(category, df):
    only_cat = df[df.category_code==category]
    return only_cat.groupby(only_cat.brand).price.mean().plot.bar(figsize = (15, 7),
                                                  title='Average Price of the products sold by each Brand in a category',
                                                                 color = 'orchid',       
                                                                 edgecolor = 'black')


'''This function first groups by the category_code and then, 
for each of them counts the average prices offered by all the 
brands inside that specific category and selects just the one 
with the highest price. The results are put inside tuples, where 
the first element is the category_code, the second the name of 
the brand and the third the avergae price offered by that specific 
brand. The tuples are then sorted in ascending order. 
From the tuples we then created a dataframe that summarized our results.'''

def highest_avg_price(df):    
    o = df.groupby(['category_code'])
    l = []
    for categ, elem in o:
        x = elem.groupby('brand').price.mean().sort_values(ascending=False).head(1)
        if x.empty==False:
            l.append((categ,x.idxmax(),round(x.max(),3)))        
        else:
            l.append((categ,0,0))
    l.sort(key=lambda tup: tup[2])
    
    out = pd.DataFrame(l)
    
    return out 

##RQ4

'''For a particular brand gave by the user in input find the monthly profit'''

def restrict_bypurchase_brand(month, brand_to_search):
    dt = month.copy() #copy the properly month dataset to do our operations 
    return int(dt[(dt.event_type == "purchase") & (dt.brand == brand_to_search)].groupby(["brand"]).price.sum()) #restrict in purchase way and we compare (to be sure) the brand, then show the profit

'''For a particular brand gave by the user in input find the monthly profit'''

def restrict_bypurchase_brand_avg(month):
    dt = month.copy() #copy the properly month dataset to do our operations 
    return dt[(dt.event_type == "purchase")].groupby(["brand"]).price.mean() #restrict in purchase way, then show the avg profit

'''Between november and october see the biggest losses'''

def big_lose(nov, octb):
    df_nov = nov.copy() #copy the properly month dataset to do our operations
    df_oct = octb.copy() #copy the properly month dataset to do our operations

    #search to undestard which 3 brands have lost more
    new_ndt_sum = df_nov[df_nov.event_type == "purchase"].groupby(["brand"]).price.sum()
    new_odt_sum = df_oct[df_oct.event_type == "purchase"].groupby(["brand"]).price.sum()

    #show the biggest losses between november and october, drop NaN values, sort to have the decreasing order and return 3 brands..
    return ((new_ndt_sum - new_odt_sum)/new_ndt_sum).dropna().sort_values(ascending=True).head(3) #(FinalValue-InitialValue/InitialValue) to calculate the variance of price then we weill multiply by 100

'''We make the foor loop to summarize our biggest losses, we multiply by 100 to obtain the percentage'''
def summarize(biggest_lose):
    for i, row in biggest_lose.iteritems():
        print('The brand', i, 'has lose the', int(abs(row*100)), '% between october and november')

##RQ5

'''function that reads only the columns needed to answer
the query inside this question'''

def import_dataset5(df_names):
    dfs = []
    for i in range(len(df_names)):
        dfs.append(pd.read_csv(df_names[i], usecols= ['event_type', 'event_time'],
                         parse_dates= ["event_time"],
                         date_parser=pd.to_datetime))
    df = pd.concat(dfs, ignore_index=True)
    return df


''' This function takes for every hour of the day the number of views there
were in the store. It then plots just the 10 highest.'''

def most_visited_time(df):     
    x = df[df.event_type == "view"].groupby([df.event_time.dt.hour]).count().event_time.sort_values(ascending=False)
    x.head(10).plot.bar()
    plt.grid(color = 'lightgray',linestyle='-.')
    plt.xlabel('Exact Time')
    plt.ylabel('Visits per hour')
    plt.title('The 10th most visited time')
    
    return plt.show()
     

''' This function takes for every day of the week the number of views there
were in the store. It then plots just the 10 highest.'''

def most_visited_day(df):
    x = df[df.event_type=="view"].groupby([df.event_time.dt.dayofweek]).count().event_time.sort_values(ascending=False)
    x.plot.bar()
    plt.grid(color = 'lightgray',linestyle='-.')
    plt.xlabel('Exact day')
    plt.ylabel('Visits per day of the week')
    plt.title('The most visited days of the week')
    
    return plt.show()
     

''' This function takes for every day of the week, the number of 
views breaked down by hour. Then, for every day we take an average 
of every hour (we divided the total per 9 weeks) and we plotted
one different graph for each of the days '''    


def avg_visitors_perday(df):
    sethourperday = df[df.event_type =="view"].groupby([df.event_time.dt.dayofweek, df.event_time.dt.hour]).event_time.count()
    
    days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    colors = ['royalblue','slateblue','darkorchid','violet','hotpink','sandybrown','salmon']
    
    for i in range(7):
        hour = []
        frequence = []
        day = i
        for group, hours in sethourperday.iteritems():
            if group[0] == i:
                hour.append(group[1])
                frequence.append(hours/9)
                
        fig = plt.figure()
        ax = fig.add_axes([0,0,1,1])
        ax.bar(hour,frequence,color = colors[i])
        ax.set_ylabel('average views')
        ax.set_xlabel('hours per day')
        ax.set_title(f'Hourly Average of visitors on {days[i]}')
        ax.grid(color = 'lightgray',linestyle='-.')
        plt.show()
    
    return

##RQ6

'''we should operate with whole datasets, so we read and concatenate all rows,
then we want to return the purchase rate value'''

def purchase_rate(df_names):
    df = loadAllDatasets(df_names, ['event_type', 'product_id']).copy() #load the datasets
    return (df[df.event_type =="purchase"].groupby([df.event_type]).product_id.count())/len(df.count()) #groupby purchase and divide all for the rows to see purchase (products) / total number rows

'''we should calculate the conversion rate...see the definition in the notebook to understand which
value we should to have'''

def conversion_rate(purchase_rate, df_names):
    df = loadAllDatasets(df_names, ['event_type', 'product_id']).copy() #load the datasets
    conversion_rate = purchase_rate[0]/(df[df.event_type =="view"].groupby([df.event_type]).product_id.count()) #groupby event_type view like the definition present in the notebook and make the operation to obtain the conversion rate
    conversion_rate = conversion_rate[0] #pick the value..
    return conversion_rate #..and return it

'''load the entire dataset and show the purchases of each category then we will show the conversion
rate per each category'''

def percategory_show_purchases(df_names):
    dt = loadAllDatasets(df_names, ['event_type', 'category_code', 'user_session'] ).copy() #load the datasets
    
    dt = dt[(dt.event_type == "purchase") & (dt.category_code.notnull())] #select the correct event_type and throw the NaN values

    dt["category"] = dt.category_code.str.split('.') #split the category_code by . and select the first element, because it is our category
    dt["category"] = dt["category"].apply(lambda x: x[0]) #extract only the first element of category_code

    dt.groupby([dt.category]).user_session.count().plot.bar(figsize=(18,6)) #groupby and show in the plot the purchases per each category, is similar to the solution of question 2
    
    #create the plot
    plt.grid()
    plt.xlabel('Categories')
    plt.ylabel('Number of Purchases')
    plt.title('Top Purchases product')
    plt.show()

'''we return the number of product per categories in our dataset, is useless but we want to have little summarize'''

def number_categories(df_names):
    df = loadAllDatasets(df_names, ['category_code', 'user_session']).copy() #load the datasets
    return df[df.category_code.notnull()].groupby("category_code").user_session.count() #return per each category the number of products that have

'''finally we obtain the conversion rate per each category as follows (see the definition of conversion rate in the notebook file)'''

def conversion_rate_percategory(df_names, eachcategory_num):
    df = loadAllDatasets(df_names, ['event_type', 'category_code']).copy() #load the datasets
    purchases = df[(df.event_type == "purchase") & (df.category_code.notnull())].groupby("category_code") #per eacj category select the product purchased and don't consider NaN values

    conversion_rate = purchases.category_code.count()/eachcategory_num #number of purchase rate divide number of categories
    return conversion_rate.sort_values(ascending=False).dropna() #return in decreasing order and dropna values


## RQ7

'''this function ranks the top 20% of the users and calculates how 
much they contributed to the total profit'''

def proof_pareto_principle(dataset):
    
    # Grouping sales within users and sort in asceding order to rank users
    total_users = dataset[dataset.event_type=='purchase'].groupby('user_id').agg(tot_sales=('price','sum'))
    total_users_sorted=total_users.sort_values("tot_sales", ascending=False)
    
    # Getting top 20% users
    top_20_users=total_users_sorted.head(int(len(total_users_sorted)*(.2)))
    display(top_20_users)
    
    # Calculate the revenue gained from 20% of users
    pur_con_20 = top_20_users.tot_sales.sum()
    
    return pur_con_20
