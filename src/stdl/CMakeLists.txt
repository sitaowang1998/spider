set(SPIDER_STDL_AST_HEADERS
    src/ast/ASTNode.hpp
    src/ast/ASTVisitor.hpp
    src/ast/FunctionNode.hpp
    src/ast/RootNode.hpp
    src/ast/StructNode.hpp
    CACHE INTERNAL
    "Spider STDL AST headers"
)

set(SPIDER_STDL_AST_SOURCES
    src/ast/ASTNode.cpp
    src/ast/ASTVisitor.cpp
    src/ast/FunctionNode.cpp
    src/ast/RootNode.cpp
    src/ast/StructNode.cpp
    CACHE INTERNAL
    "Spider STDL AST sources"
)

add_library(spider_stdl_ast)
target_sources(spider_stdl_ast PRIVATE ${SPIDER_STDL_AST_SOURCES})
target_include_directories(spider_stdl_ast PUBLIC ..)
target_sources(spider_stdl_ast PUBLIC ${SPIDER_STDL_AST_HEADERS})
target_link_libraries(spider_stdl_ast PUBLIC antlr4-runtime)
