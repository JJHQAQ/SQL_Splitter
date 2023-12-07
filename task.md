# 根据需要的功能划分

1. select * from customer
    customer表水平划分
2. select title from book
    book表垂直划分
3. select * from book where copies>3000 and publisher_id = 101085
   book表垂直划分
4. select book_id,quantity from orders
   order表水平划分
5. select book.title,book.authors 
   from book,publisher 
   where book.publisher_id = publisher.id and publisher.name="Twomorrows Publishing" and book.copies>6000
    多表连接测试
6. select customer.name from customer,orders
   where customer.id = orders.customer_id
