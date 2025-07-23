#include "StructNode.hpp"

#include <stdl/src/ast/ASTNode.hpp>

namespace stdl::ast {
StructNode::StructNode(int const line, int const row) : ASTNode{SourceLocation{line, row}} {}

auto StructNode::accept(ASTVisitor& visitor) -> void {
    for (std::pair<std::unique_ptr<TypeNode>, std::unique_ptr<IdNode>>& field : m_fields) {
        field.first->accept(visitor);
        field.second->accept(visitor);
    }
}

auto StructNode::get_parent() const -> RootNode* {
    return m_parent;
}

auto StructNode::set_parent(RootNode* parent) -> void {
    m_parent = parent;
}

auto StructNode::add_field(std::unique_ptr<TypeNode> type, std::unique_ptr<IdNode> id) -> void {
    m_fields.emplace_back(std::move(type), std::move(id));
}
}  // namespace stdl::ast
