#ifndef STDL_AST_STRUCTNODE_HPP
#define STDL_AST_STRUCTNODE_HPP

#include <memory>
#include <utility>
#include <vector>

#include <stdl/src/ast/ASTNode.hpp>

namespace stdl::ast {
class RootNode;
class TypeNode;
class IdNode;

class StructNode : public ASTNode {
public:
    StructNode(int line, int row);

    auto accept(ASTVisitor& visitor) -> void override;

    auto add_field(std::unique_ptr<TypeNode> type, std::unique_ptr<IdNode> id) -> void;

    auto set_parent(RootNode* parent) -> void;

    [[nodiscard]] auto get_parent() const -> RootNode*;

private:
    RootNode* m_parent = nullptr;

    std::vector<std::pair<std::unique_ptr<TypeNode>, std::unique_ptr<IdNode>>> m_fields;
};
}  // namespace stdl::ast

#endif
