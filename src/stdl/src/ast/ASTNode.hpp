#ifndef STDL_AST_ASTNODE_HPP
#define STDL_AST_ASTNODE_HPP

#include <stdl/src/ast/ASTVisitor.hpp>

namespace stdl::ast {
class ASTVisitor;

class SourceLocation {
public:
    SourceLocation(int line, int row);

    [[nodiscard]] auto get_line() const -> int;

    [[nodiscard]] auto get_row() const -> int;

private:
    int m_line = -1;
    int m_row = -1;
};

class ASTNode {
public:
    virtual auto accept(ASTVisitor& visitor) -> void = 0;
    virtual ~ASTNode() = default;

    explicit ASTNode(SourceLocation const location);

    [[nodiscard]] auto get_location() const -> SourceLocation const&;

private:
    SourceLocation m_location;
};
};  // namespace stdl::ast

#endif
