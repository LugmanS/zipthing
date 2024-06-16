export function genericInternalErrorResponse(requestId: string) {
  return {
    requestId,
    status: "error",
    message: "Internal server error",
  };
}
