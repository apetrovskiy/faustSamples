import faust

class Order(faust.Record):
    account_id:str
    product_id:str
    amount:int
    price:float

app = faust.App('hello-app', broker='kafka://localhost')
orders_kafka_topic =app.topic('orders',value_type=Order)

order_count_by_account=app.Table('order_count',default=int)

# @app.agent(value_type=Order)
# async def order(orders):
#     async for order in orders:
#         print(f'Order for {order.account_id}: {order.amount}')
#         order(order)

@app.agent(orders_kafka_topic)
async def process(orders:faust.Stream[Order])->None:
    async for order in orders.group_by(Order.account_id):
        order_count_by_account[order.account_id]+=1
