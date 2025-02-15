import ast
import logging
import traceback
from collections import defaultdict
from typing import Any, Callable, Generator, List, Union

from pandasai.exceptions import InvalidLLMOutputType, InvalidOutputValueMismatch
from pandasai.pipelines.logic_unit_output import LogicUnitOutput
from pandasai.responses.response_serializer import ResponseSerializer

from ...exceptions import NoResultFoundError
from ...helpers.logger import Logger
from ...helpers.node_visitors import AssignmentVisitor, CallVisitor
from ...helpers.optional import get_environment
from ...helpers.output_validator import OutputValidator
from ...schemas.df_config import Config
from ..base_logic_unit import BaseLogicUnit
from ..pipeline_context import PipelineContext
from .code_cleaning import CodeExecutionContext


class CodeExecution(BaseLogicUnit):
    """
    Code Execution Stage
    """

    _dfs: List
    _config: Union[Config, dict]
    _additional_dependencies: List[dict] = []
    _current_code_executed: str = None
    _retry_if_fail: bool = False
    _ast_comparator_map: dict = {
        ast.Eq: "=",
        ast.NotEq: "!=",
        ast.Lt: "<",
        ast.LtE: "<=",
        ast.Gt: ">",
        ast.GtE: ">=",
        ast.Is: "is",
        ast.IsNot: "is not",
        ast.In: "in",
        ast.NotIn: "not in",
    }

    def __init__(
        self,
        on_failure: Callable[[str, Exception], None] = None,
        on_retry: Callable[[str, Exception], None] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.on_failure = on_failure
        self.on_retry = on_retry

    def execute(self, input: Any, **kwargs) -> Any:
        """
        This method will return output according to
        Implementation.

        :param input: Your input data.
        :param kwargs: A dictionary of keyword arguments.
            - 'logger' (any): The logger for logging.
            - 'config' (Config): Global configurations for the test
            - 'context' (any): The execution context.

        :return: The result of the execution.
        """
        self.context: PipelineContext = kwargs.get("context")
        self._dfs = self.context.dfs
        self._config = self.context.config
        self._additional_dependencies = self.context.get("additional_dependencies", [])
        self._current_code_executed = self.context.get("current_code_executed")
        self.logger: Logger = kwargs.get("logger")

        # Execute the code
        code_context = CodeExecutionContext(
            self.context.get("last_prompt_id"), self.context.skills_manager
        )

        retry_count = 0
        code_to_run = input
        result = None
        while retry_count <= self.context.config.max_retries:
            try:
                if(self.context.enforce_grouping and ".groupby" not in code_to_run):
                    exc = "groupby() statement is missing. Group the data by "
                    # Find all the dimension columns in self._dfs
                    for df in self._dfs:
                        for col in df.pandas_df.columns:
                            if df.pandas_df[col].dtype == 'object' and "'" + col + "'" in code_to_run:
                                exc = exc + "'" + col + "', "
                    raise Exception(exc)


                result = self.execute_code(code_to_run, code_context)
                if self.context.get("output_type") != "" and (
                    output_helper := self.context.get("output_type")
                ):
                    (validation_ok, validation_errors) = OutputValidator.validate(
                        output_helper, result
                    )

                    if not validation_ok:
                        raise InvalidLLMOutputType(validation_errors)

                if not OutputValidator.validate_result(result):
                    raise InvalidOutputValueMismatch(
                        f'Value type {type(result["value"])} must match with type {result["type"]}'
                    )

                break

            except Exception as e:
                traceback_errors = traceback.format_exc()
                self.logger.log(f"Failed with error: {traceback_errors}", logging.ERROR)
                if self.on_failure:
                    self.on_failure(code_to_run, traceback_errors)

                if (
                    not self.context.config.use_error_correction_framework
                    or retry_count >= self.context.config.max_retries
                ):
                    raise e

                retry_count += 1

                self.logger.log(
                    f"Failed to execute code retrying with a correction framework "
                    f"[retry number: {retry_count}]",
                    level=logging.WARNING,
                )

                # TODO - Move this implement to main execute function
                # Temporarily done for test cases this is to be fixed move to the main function
                code_to_run = self._retry_run_code(
                    code_to_run, self.context, self.logger, e
                )

        return LogicUnitOutput(
            result,
            True,
            "Code Executed Successfully",
            {"content_type": "response", "value": ResponseSerializer.serialize(result)},
            final_track_output=True,
        )

    def execute_code(self, code: str, context: CodeExecutionContext) -> Any:
        """
        Execute the python code generated by LLMs to answer the question
        about the input dataframe. Run the code in the current context and return the
        result.

        Args:
            code (str): Python code to execute.
            context (CodeExecutionContext): Code Execution Context
                    with prompt id and skills.

        Returns:
            Any: The result of the code execution. The type of the result depends
                on the generated code.

        """
        # List the required dfs, so we can avoid to run the connectors
        # if the code does not need them
        dfs = self._required_dfs(code)
        environment: dict = get_environment(self._additional_dependencies)
        environment["dfs"] = self._get_originals(dfs)
        if len(environment["dfs"]) == 1:
            environment["df"] = environment["dfs"][0]

        if self._config.direct_sql:
            environment["execute_sql_query"] = self._dfs[0].execute_direct_sql_query

        # Add skills to the env
        if context.skills_manager.used_skills:
            for skill_func_name in context.skills_manager.used_skills:
                skill = context.skills_manager.get_skill_by_func_name(skill_func_name)
                environment[skill_func_name] = skill

        # Execute the code
        exec(code, environment)

        # Get the result
        if "result" not in environment:
            raise NoResultFoundError("No result returned")

        return environment["result"]

    def _required_dfs(self, code: str) -> List[str]:
        """
        List the index of the DataFrames that are needed to execute the code. The goal
        is to avoid to run the connectors if the code does not need them.

        Args:
            code (str): Python code to execute

        Returns:
            List[int]: A list of the index of the DataFrames that are needed to execute
            the code.
        """

        # Sometimes GPT-3.5/4 use a for loop to iterate over the dfs (even if there is only one)
        # or they concatenate the dfs. In this case we need all the dfs
        if "for df in dfs" in code or "pd.concat(dfs" in code:
            return self._dfs

        required_dfs = []
        for i, df in enumerate(self._dfs):
            if f"dfs[{i}]" in code:
                required_dfs.append(df)
            else:
                required_dfs.append(None)
        return required_dfs or self._dfs

    def _get_originals(self, dfs):
        """
        Get original dfs

        Args:
            dfs (list): List of dfs

        Returns:
            list: List of dfs
        """
        original_dfs = []
        for index, df in enumerate(dfs):
            if df is None:
                original_dfs.append(None)
                continue

            extracted_filters = self._extract_filters(self._current_code_executed)
            filters = extracted_filters.get(f"dfs[{index}]", [])
            df.set_additional_filters(filters)

            df.execute()
            # df.load_connector(partial=len(filters) > 0)

            original_dfs.append(df.pandas_df)

        return original_dfs

    def _extract_filters(self, code) -> dict[str, list]:
        """
        Extract filters to be applied to the dataframe from passed code.

        Args:
            code (str): A snippet of code to be parsed.

        Returns:
            dict: The dictionary containing all filters parsed from
                the passed code. The dictionary has the following structure:
                {
                    "<df_number>": [
                        ("<left_operand>", "<operator>", "<right_operand>")
                    ]
                }

        Raises:
            SyntaxError: If the code is unable to be parsed by `ast.parse()`.
            Exception: If any exception is raised during working with nodes
                of the code tree.
        """
        try:
            parsed_tree = ast.parse(code)
        except SyntaxError:
            self.logger.log(
                "Invalid code passed for extracting filters", level=logging.ERROR
            )
            self.logger.log(f"{traceback.format_exc()}", level=logging.DEBUG)
            raise

        try:
            filters = self._extract_comparisons(parsed_tree)
        except Exception:
            self.logger.log(
                "Unable to extract filters for passed code", level=logging.ERROR
            )
            self.logger.log(f"Error: {traceback.format_exc()}", level=logging.DEBUG)
            return {}

        return filters

    def _extract_comparisons(self, tree: ast.Module) -> dict[str, list]:
        """
        Process nodes from passed tree to extract filters.

        Collects all assignments in the tree.
        Collects all function calls in the tree.
        Walk over the tree and handle each comparison node.
        For each comparison node, defined what `df` is this node related to.
        Parse constants values from the comparison node.
        Add to the result dict.

        Args:
            tree (str): A snippet of code to be parsed.

        Returns:
            dict: The `defaultdict(list)` instance containing all filters
                parsed from the passed instructions tree. The dictionary has
                the following structure:
                {
                    "<df_number>": [
                        ("<left_operand>", "<operator>", "<right_operand>")
                    ]
                }
        """
        comparisons = defaultdict(list)
        current_df = "dfs[0]"

        visitor = AssignmentVisitor()
        visitor.visit(tree)
        assignments = visitor.assignment_nodes

        call_visitor = CallVisitor()
        call_visitor.visit(tree)

        for node in ast.walk(tree):
            if isinstance(node, ast.Compare) and isinstance(node.left, ast.Subscript):
                name, *slices = self._tokenize_operand(node.left)
                current_df = (
                    self._get_df_id_by_nearest_assignment(
                        node.lineno, assignments, name
                    )
                    or current_df
                )
                left_str = slices[-1] if slices else name

                for op, right in zip(node.ops, node.comparators):
                    op_str = self._ast_comparator_map.get(type(op), "Unknown")
                    name, *slices = self._tokenize_operand(right)
                    right_str = slices[-1] if slices else name

                    comparisons[current_df].append((left_str, op_str, right_str))
        return comparisons

    def _retry_run_code(
        self,
        code: str,
        context: PipelineContext,
        logger: Logger,
        e: Exception,
    ) -> str:
        """
        A method to retry the code execution with error correction framework.

        Args:
            code (str): A python code
            context (PipelineContext) : Pipeline Context
            logger (Logger) : Logger
            e (Exception): An exception
            dataframes

        Returns (str): A python code
        """
        if self.on_retry:
            return self.on_retry(code, e)
        else:
            raise e

    @staticmethod
    def _tokenize_operand(operand_node: ast.expr) -> Generator[str, None, None]:
        """
        Utility generator function to get subscript slice constants.

        Args:
            operand_node (ast.expr):
                The node to be tokenized.
        Yields:
            str: Token string.

        Examples:
            >>> code = '''
            ... foo = [1, [2, 3], [[4, 5], [6, 7]]]
            ... print(foo[2][1][0])
            ... '''
            >>> tree = ast.parse(code)
            >>> res = CodeManager._tokenize_operand(tree.body[1].value.args[0])
            >>> print(list(res))
            ['foo', 2, 1, 0]
        """
        if isinstance(operand_node, ast.Call):
            yield operand_node.func.attr

        if isinstance(operand_node, ast.Subscript):
            slice_ = operand_node.slice.value
            yield from CodeExecution._tokenize_operand(operand_node.value)
            yield slice_

        if isinstance(operand_node, ast.Name):
            yield operand_node.id

        if isinstance(operand_node, ast.Constant):
            yield operand_node.value

    @staticmethod
    def _get_nearest_func_call(current_lineno, calls, func_name):
        """
        Utility function to get the nearest previous call node.

        Sort call nodes list (copy of the list) by line number.
        Iterate over the call nodes list. If the call node's function name
        equals to `func_name`, set `nearest_call` to the node object.

        Args:
            current_lineno (int): Number of the current processed line.
            calls (list[ast.Assign]): List of call nodes.
            func_name (str): Name of the target function.

        Returns:
            ast.Call: The node of the nearest previous call `<func_name>()`.
        """
        for call in reversed(calls):
            if call.lineno < current_lineno:
                try:
                    if call.func.attr == func_name:
                        return call
                except AttributeError:
                    continue

        return None

    @staticmethod
    def _get_df_id_by_nearest_assignment(
        current_lineno: int, assignments: list[ast.Assign], target_name: str
    ):
        """
        Utility function to get df label by finding the nearest assignment.

        Sort assignment nodes list (copy of the list) by line number.
        Iterate over the assignment nodes list. If the assignment node's value
        looks like `dfs[<index>]` and target label equals to `target_name`,
        set `nearest_assignment` to "dfs[<index>]".

        Args:
            current_lineno (int): Number of the current processed line.
            assignments (list[ast.Assign]): List of assignment nodes.
            target_name (str): Name of the target variable. The assignment
                node is supposed to assign to this name.

        Returns:
            str: The string representing df label, looks like "dfs[<index>]".
        """
        nearest_assignment = None
        assignments = sorted(assignments, key=lambda node: node.lineno)
        for assignment in assignments:
            if assignment.lineno > current_lineno:
                return nearest_assignment
            try:
                is_subscript = isinstance(assignment.value, ast.Subscript)
                dfs_on_the_right = assignment.value.value.id == "dfs"
                assign_to_target = assignment.targets[0].id == target_name
                if is_subscript and dfs_on_the_right and assign_to_target:
                    nearest_assignment = f"dfs[{assignment.value.slice.value}]"
            except AttributeError:
                continue
