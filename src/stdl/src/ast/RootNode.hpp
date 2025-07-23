#ifndef STDL_AST_ROOTNODE_HPP
#define STDL_AST_ROOTNODE_HPP

#include <memory>
#include <vector>

#include <stdl/src/ast/ASTNode.hpp>

namespace stdl::ast {
class FunctionNode;
class StructNode;

class RootNode : public ASTNode {
public:
    auto accept(ASTVisitor& visitor) -> void override;

    auto add_struct(std::unique_ptr<StructNode> struct_node) -> void;

    auto add_function(std::unique_ptr<FunctionNode> function_node) -> void;

private:
    std::vector<std::unique_ptr<StructNode>> m_structs;
    std::vector<std::unique_ptr<FunctionNode>> m_functions;
};
}  // namespace stdl::ast

#endif
