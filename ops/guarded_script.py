"""Keep generated helper guards active under optimized Python."""
import ast


def explicit_guard_source(source):
    """Replace each assert with an equivalent explicit AssertionError guard."""
    class ReplaceAssertions(ast.NodeTransformer):
        def visit_Assert(self, node):
            return ast.copy_location(ast.If(test=ast.UnaryOp(op=ast.Not(), operand=self.visit(node.test)),
                body=[ast.Raise(exc=ast.Call(func=ast.Name(id='AssertionError', ctx=ast.Load()),
                    args=[] if node.msg is None else [self.visit(node.msg)], keywords=[]), cause=None)], orelse=[]), node)
    return ast.unparse(ast.fix_missing_locations(ReplaceAssertions().visit(ast.parse(source))))
