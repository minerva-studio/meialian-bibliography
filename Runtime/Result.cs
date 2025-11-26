using System;

namespace Minerva.DataStorage
{
    /// <summary>
    /// Result of an operation: success or failure with error message.
    /// </summary>
    public readonly struct Result : IEquatable<Result>
    {
        public static readonly Result Succeeded = new(true, null);

        public readonly bool Success;
        public readonly string ErrorMessage;

        public Result(bool success, string errorMessage)
        {
            Success = success;
            ErrorMessage = errorMessage;
        }

        public static Result Failed(string message) => new Result(false, message);
        public static Result<T> Succeed<T>(T value) => new Result<T>(value);
        public static Result<T> Failed<T>(string message) => new Result<T>(message);


        public bool Equals(Result other) => Success == other.Success && ErrorMessage == other.ErrorMessage;
        public override bool Equals(object obj) => obj is Result other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Success, ErrorMessage);
        public override string ToString() => Success ? "Success" : $"Failed: {ErrorMessage}";
        public static bool operator ==(Result left, Result right) => left.Equals(right);
        public static bool operator !=(Result left, Result right) => !(left == right);
        public static Result operator &(Result left, Result right) => !left.Success ? left : right;
        public static Result operator |(Result left, Result right) => left.Success ? left : right;
        public static bool operator true(Result result) => result.Success;
        public static bool operator false(Result result) => !result.Success;

        public static implicit operator bool(Result result) => result.Success;

        public void ThrowIfFailed()
        {
            if (!Success)
                throw new InvalidOperationException(ErrorMessage ?? "Operation failed.");
        }
    }

    /// <summary>
    /// Result of an operation: success or failure with error message.
    /// </summary>
    public readonly struct Result<T> : IEquatable<Result<T>>
    {
        public readonly bool Success;
        public readonly T Value;
        public readonly string ErrorMessage;

        public Result(string errorMessage)
        {
            Value = default;
            Success = false;
            ErrorMessage = errorMessage;
        }

        public Result(T value)
        {
            Value = value;
            Success = true;
            ErrorMessage = null;
        }

        public bool Equals(Result<T> other) => Success == other.Success && ErrorMessage == other.ErrorMessage;
        public override bool Equals(object obj) => obj is Result<T> other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Success, ErrorMessage);
        public override string ToString() => Success ? $"Success: {Value}" : $"Failed: {ErrorMessage}";
        public static bool operator ==(Result<T> left, Result<T> right) => left.Equals(right);
        public static bool operator !=(Result<T> left, Result<T> right) => !(left == right);
        public static Result<T> operator &(Result<T> left, Result<T> right) => !left.Success ? left : right;
        public static Result<T> operator |(Result<T> left, Result<T> right) => left.Success ? left : right;
        public static bool operator true(Result<T> result) => result.Success;
        public static bool operator false(Result<T> result) => !result.Success;

        public static implicit operator bool(Result<T> result) => result.Success;
        public static implicit operator Result<T>(T value) => new Result<T>(value);
        public static implicit operator Result(Result<T> result) => result.Success ? Result.Succeeded : Result.Failed(result.ErrorMessage);

        public void ThrowIfFailed()
        {
            if (!Success)
                throw new InvalidOperationException(ErrorMessage ?? "Operation failed.");
        }
    }
}
