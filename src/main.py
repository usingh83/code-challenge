from dateutil.parser import parse as date_parser
from dateutil import rrule
import MySQLdb

# Select if you want to see the SQL solution or the Python one
SQL=False

# Function to count the weeks between 2 dates, if 2 Dates are equal count_weeks is 1
def weeks_between_dates(start, end):
    weeks = rrule.rrule(rrule.WEEKLY, dtstart=start, until=end)
    return weeks.count()

# Function to read the data from file line by line and inputs the line for ingestion
def filereader(file_path, events):
    first_loop = True # to remove the open square bracket in the first line
    with open(file_path) as f:
        for line in f.readlines():
            if first_loop:
                #first line extracted
                first_loop = False
                line_eval = line.strip()[1:-1]
            else:
                #the rest of the lines extracted
                line_eval = line.strip()[:-1]
            if SQL:
                ingestSQL(line_eval)
            else:
                ingest(line_eval, events)

# Function to write the output to the output file
def write_output(fname, data):
    with open(fname, 'w') as f:
        for x in data:
            f.write(x[0] + ', ' + str(x[1]) + '\n')

# Function to ingest the line and assing them to a Customer
def ingest(e, D):
    dic = eval(e)
    # eval will convert the data into a python Dictionary
    if 'event_time' in dic:
        # to parse the date
        dic['event_time'] = date_parser(dic['event_time'])
    # extracting the customer id
    customer_id = dic['customer_id'] if dic['type'] != 'CUSTOMER' else dic['key']
    if customer_id not in D:
        # Create new customer entry
        D[customer_id] = [dic]
    else:
        # Add to present customer's data
        D[customer_id].append(dic)
# Function to ingest the line and assign them to a Customer using SQL
def ingestSQL(e):
    dic = eval(e)
    # eval will convert the data into a python Dictionary
    conn = MySQLdb.connect(host='localhost', user='root', passwd='Admin123')
    cursor = conn.cursor()
    cursor.execute('use Library')
    # connecting to the database
    if 'event_time' in dic:
        dic['event_time'] = date_parser(dic['event_time'])
        # parsing date
    if dic['type']=='CUSTOMER':
        #insertion script for customer table
        sql = "INSERT INTO CUSTOMER VALUES('%s','%s','%s','%s','%s')" % \
              (dic['verb'], dic['key'], dic['event_time'].date(), dic.get('adr_city',None), dic['adr_state'])
        print(sql)
        cursor.execute(sql)
        conn.commit()
    if dic['type']=='SITE_VISIT':
        #insertion script for SITE_VISIT table
        sql = "INSERT INTO SITE_VISIT VALUES('%s','%s','%s','%s')" % \
              (dic['verb'], dic['key'], dic['event_time'].date(), dic['customer_id'])
        print(sql)
        cursor.execute(sql)
        conn.commit()
    if dic['type']=='IMAGE':
        # insertion script for IMAGE table
        sql = "INSERT INTO IMAGE VALUES('%s','%s','%s','%s','%s','%s')" % \
              (dic['verb'], dic['key'], dic['event_time'].date(), dic['customer_id'], dic.get('camera_make',None), dic.get('camera_model',None))
        print(sql)
        cursor.execute(sql)
        conn.commit()
    if dic['type']=='ORDER':
        # insertion script for ORDERS table
        sql = "INSERT INTO ORDERS VALUES('%s','%s','%s','%s','%.2f')" % \
              (dic['verb'], dic['key'], dic['event_time'].date(), dic['customer_id'], float(dic['total_amount'].split()[0]))
        print(sql)
        cursor.execute(sql)
        conn.commit()
    cursor.close()

# top X simple LTV customers using SQL
def topXSimpleLTVCustomersSQL(x, print_info=False):
    """
    calculation for LTV:
    LTV = (average revenue / week) * (52 weeks / year) * 10 years
    """
    LTVs = []
    conn = MySQLdb.connect(host='localhost', user='root', passwd='Admin123')
    cursor = conn.cursor()
    cursor.execute('use Library')
    # connecting to the database
    cursor.execute('''  /*This outer query sums up the values of the 3 inner queries to generate the desired results*/
                        Select cust_id,sum(amount),sum(weeks)
                        From
                        (Select X.cust_id ,X.amount,Y.weeks from
                        (
                        /*This inner query will show the order amount of the latest update to a Order key summed up for 
                        each customer*/
                        SELECT A.list_customer_id cust_id,
                        sum(A.list_total_amount) amount
                        FROM Orders A
                        WHERE NOT EXISTS
                        (SELECT 1 
                        FROM Orders B 
                        WHERE B.list_event_time > A.list_event_time 
                        AND A.list_key = B.list_key 
                        and B.list_customer_id=A.list_customer_id 
                        )group by A.list_customer_id) X,
                        (
                        /*This is the 2nd subquery which shows the maximum week difference between the customers 
                        visited*/
                        SELECT A.list_customer_id cust_id,
                        Round(datediff(max(A.list_event_time),min(A.list_event_time))/7)+1 weeks
                        FROM Site_Visit A   
                        group by A.list_customer_id) Y  
                        where X.cust_id=Y.cust_id
                        union
                        /* This is the 3rd subsquery, which gets the customers who were filtered in the join */
                        select distinct D.list_key ,0 ,0  from CUSTOMER D) Z
                        group by Z.cust_id''')
    data = cursor.fetchall()
    cursor.close()
    # print the rows
    for row in data:
        # iterate through the query's results
        cid, amounts, week = row[0], row[1], row[2]
        if int(week) == 0:
            # if customers have not visited
            LTVs.append((cid, 0))
            continue
        avg = float(amounts)/float(week)
        LTVs.append((cid, round(52*avg*10, 2)))

    LTVs.sort(reverse=True, key=lambda y: y[1])
    if print_info:
        #print the results
        print "\nFull LTV list:"
        for ltv in LTVs:
            print "{}".format(ltv)

    return LTVs[:x]

#pyhon based function to get to X top LTV customer
def topXSimpleLTVCustomers(x, D, print_info=False):
    """
    calculation of LTV:
    LTV = (average revenue / week) * (52 weeks / year) * 10 years
    """

    LTVs = []
    for customer_id in D:

        # VISITS PER WEEK
        # If no SITE_VISIT events exist but there are customer orders,
        # possibly due to corrupt data, interpret ORDER events as visits as backup.
        vkey = 'SITE_VISIT' if 'SITE_VISIT' in [r['type'] for r in D[customer_id]] else 'ORDER'
        visits_dates_list = [r['event_time'] for r in D[customer_id] if r['type'] == vkey]
        # Check customer record has ORDER and SITE_VISIT events
        if visits_dates_list and 'ORDER' in [r['type'] for r in D[customer_id]]:
            weeks = weeks_between_dates(min(visits_dates_list), max(visits_dates_list))

            # CUSTOMER EXPENDITURE PER VISIT
            order_data = [ (r['key'], r['verb'], r['event_time'], float(r['total_amount'].split()[0]))
                           for r in D[customer_id] if r['type'] == 'ORDER' ]
            order_amounts_by_id = {}
            # Check for order updates
            for k, verb, ev_dt, amount in order_data:
                if k not in order_amounts_by_id:
                    order_amounts_by_id[k] = (ev_dt, amount)
                else:
                    if ev_dt > order_amounts_by_id[k][0]:
                        # Replace amount if newer update exists
                        order_amounts_by_id[k] = (ev_dt, amount)
            amount = sum([order_amounts_by_id[k][1] for k in order_amounts_by_id])
            avg = float(amount) / weeks

            # LTV
            LTVs.append( (customer_id, 52 * avg * 10) )
        else:
            # No ORDER events
            LTVs.append( (customer_id, 0) )

    LTVs.sort(reverse=True, key=lambda y: y[1])
    if print_info:
        print "\nFull LTV list:"
        for ltv in LTVs:
            print "{}".format(ltv)

    return LTVs[:x]

if __name__ == '__main__':
    if SQL:
        #SQL script to ready the database
        conn = MySQLdb.connect(host='localhost', user='root', passwd='Admin123')
        cursor = conn.cursor()
        cursor.execute('Create database Library')
        cursor.execute('use Library')
        cursor.execute('CREATE TABLE IF NOT EXISTS CUSTOMER (`list_verb` VARCHAR(6) , `list_key` VARCHAR(20) ,`list_event_time` Date ,`list_adr_city` VARCHAR(20) ,`list_adr_state` VARCHAR(20) )')
        cursor.execute('CREATE TABLE IF NOT EXISTS SITE_VISIT (`list_verb` VARCHAR(6) ,`list_key` VARCHAR(20) ,`list_event_time` Date ,`list_customer_id` VARCHAR(20) )')
        cursor.execute('CREATE TABLE IF NOT EXISTS IMAGE (`list_verb` VARCHAR(6) ,`list_key` VARCHAR(20) ,`list_event_time` Date ,`list_customer_id` VARCHAR(20) ,`list_camera_make` VARCHAR(15) ,`list_camera_model` VARCHAR(15) )')
        cursor.execute('CREATE TABLE IF NOT EXISTS ORDERS (`list_verb` VARCHAR(6) ,`list_key` VARCHAR(20) ,`list_event_time` Date ,`list_customer_id` VARCHAR(20) ,`list_total_amount` FLOAT(9) )')
    customer_info = {}
    print_info = True
    filereader("../input/input.txt", customer_info)
    if SQL:
        top_LTVs = topXSimpleLTVCustomersSQL(10, print_info)
    else:
        top_LTVs = topXSimpleLTVCustomers(10, customer_info, print_info)
    output_file = "../output/output.txt"
    write_output(output_file, top_LTVs)
    print "\nData saved in: {}".format(output_file)
    if SQL:
        cursor.execute('Drop database Library')
        cursor.close()