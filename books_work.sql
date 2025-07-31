SELECT * FROM books_db.book_info;

SELECT 
    bi.Title, 
    bi.Availability
FROM 
    books_db.book_info AS bi
WHERE 
    bi.Availability = 'In Stock'
ORDER BY 
    bi.Rating DESC
LIMIT 10;