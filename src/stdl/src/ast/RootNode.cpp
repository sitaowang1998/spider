#include "RootNode.hpp"

#include <stdl/src/ast/FunctionNode.hpp>
#include <stdl/src/ast/StructNode.hpp>

namespace stdl::ast {
auto RootNode::accept(ASTVisitor& visitor) -> void {
    for (std::unique_ptr<StructNode>& node : m_structs) {
        node->accept(visitor);
    }
    for (std::unique_ptr<FunctionNode>& node : m_functions) {
        node->accept(visitor);
    }
}

auto RootNode::add_function(std::unique_ptr<FunctionNode> function_node) -> void {
    m_functions.emplace_back(std::move(function_node));
}

auto RootNode::add_struct(std::unique_ptr<StructNode> struct_node) -> void {
    struct_node->set_parent(this);
    m_structs.emplace_back(std::move(struct_node));
}
}  // namespace stdl::ast

